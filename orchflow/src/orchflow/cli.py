"""TRACK 3 owns this file. The `orchflow` command.

    orchflow plugins                                  list registered plug-ins
    orchflow run EXPERIMENT.yaml [--seeds N]          run an experiment (sim or live)
    orchflow agent EXPERIMENT.yaml --nodes coord      start ONE live process (used by Docker)
    orchflow merge-logs RUN_DIR                       events-*.jsonl -> events.jsonl
    orchflow table1 RESULTS_DIR                       Table I from event logs (Track 2's code)
    orchflow import-legacy WORKFLOW.xml -o OUT.yaml   (Track 1 writes the importer)

Contracts with the other tracks (agree on these names on the group call):
    Track 2, sim runtime : orchflow.runtime.sim.run_seed(experiment, seed, run_dir) -> None
    Track 2, metrics     : orchflow.metrics.table1.main(argv: list[str]) -> int
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import sys
import time
from pathlib import Path

from orchflow.core import plugins

log = logging.getLogger("orchflow")


def _parse_broker(text: str) -> tuple[str, int]:
    host, _, port = text.partition(":")
    return host or "localhost", int(port or 1883)


def _setup_logging(quiet: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if quiet:
        logging.getLogger("orchflow.live.msg").setLevel(logging.WARNING)


def _asyncio_run(coro):  # type: ignore[no-untyped-def]
    # aiomqtt needs the selector event loop; Windows defaults to the proactor loop.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(coro)


def _optional(module: str):  # type: ignore[no-untyped-def]
    """Import another track's module, or None if they haven't written it yet."""
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as e:
        if e.name and module.startswith(e.name):
            return None
        raise


# ------------------------------------------------------------------ commands


def cmd_plugins(_: argparse.Namespace) -> int:
    for kind in plugins.PLUGIN_KINDS:
        print(f"{kind:14} {', '.join(plugins.available(kind)) or '-'}")
    return 0


async def _live_agent(experiment: Path, nodes: str, broker: str, seed: int, run_dir: Path,
                      name: str, duration_s: float | None, ready_timeout_s: float,
                      mobility: bool) -> Path:
    from orchflow.runtime.live.agent import (
        LiveAgent,
        build_registry,
        load_inputs,
        load_mobility,
        select_nodes,
    )
    from orchflow.runtime.live.transport import LocalBroker, MqttTransport, Transport

    exp, wf, testbed = load_inputs(experiment, duration_s=duration_s)
    registry = build_registry(testbed)
    transport: Transport
    if broker == "memory":
        transport = LocalBroker().transport()
    else:
        host, port = _parse_broker(broker)
        transport = MqttTransport(host, port, client_id=f"orchflow-{name}-{time.time_ns()}")
    agent = LiveAgent(experiment=exp, workflow=wf, registry=registry,
                      nodes=select_nodes(nodes, registry), transport=transport,
                      run_dir=run_dir, seed=seed, name=name,
                      mobility=load_mobility(exp) if mobility else None,
                      ready_timeout_s=ready_timeout_s)
    return await agent.run()


def cmd_agent(a: argparse.Namespace) -> int:
    name = a.name or a.nodes.replace(",", "+").replace("@", "-")
    _asyncio_run(_live_agent(a.experiment, a.nodes, a.broker, a.seed, a.run_dir, name,
                             a.duration, a.ready_timeout, not a.no_mobility))
    return 0


def cmd_run(a: argparse.Namespace) -> int:
    from orchflow.core.spec import ExperimentSpec, load_yaml

    exp = load_yaml(a.experiment, ExperimentSpec)
    seeds = list(range(a.seeds)) if a.seeds is not None else exp.seeds
    out = Path(a.out) if a.out else exp.output_dir
    base = out / f"{exp.name}-{time.strftime('%Y%m%d-%H%M%S')}"

    if exp.runtime == "sim":
        sim = _optional("orchflow.runtime.sim")
        if sim is None or not hasattr(sim, "run_seed"):
            print("the sim runtime isn't written yet (Track 2: orchflow.runtime.sim.run_seed)",
                  file=sys.stderr)
            return 2
        if a.duration is not None:
            exp = exp.model_copy(update={"duration_s": a.duration})
        for s in seeds:
            sim.run_seed(exp, s, base / f"seed-{s:03d}")
        print(f"results in {base}")
        return 0

    # live: every node in this one process, talking through the broker
    from orchflow.runtime.live.logs import merge_events

    for s in seeds:
        run_dir = base / f"seed-{s:03d}"
        _asyncio_run(_live_agent(Path(a.experiment), "all", a.broker, s, run_dir, "all",
                                 a.duration, 10.0, not a.no_mobility))
        print(f"seed {s}: {merge_events(run_dir)}")
    return 0


def cmd_merge_logs(a: argparse.Namespace) -> int:
    from orchflow.runtime.live.logs import merge_events

    print(merge_events(a.run_dir))
    return 0


def cmd_table1(a: argparse.Namespace) -> int:
    from orchflow.runtime.live.logs import merge_events

    results = Path(a.results)
    # live runs leave one file per agent: merge them first
    for d in {p.parent for p in results.rglob("events-*.jsonl")}:
        if not (d / "events.jsonl").exists():
            merge_events(d)
    table1 = _optional("orchflow.metrics.table1")
    if table1 is None:
        print("metrics/table1.py isn't written yet (Track 2)", file=sys.stderr)
        return 2
    return int(table1.main([str(results)]) or 0)


# ------------------------------------------------------------------ parser


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="orchflow")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("plugins", help="list registered plug-ins").set_defaults(fn=cmd_plugins)

    r = sub.add_parser("run", help="run an experiment YAML (sim or live)")
    r.add_argument("experiment", type=Path)
    r.add_argument("--seeds", type=int, help="run seeds 0..N-1 (default: from the YAML)")
    r.add_argument("--duration", type=float, help="override duration_s")
    r.add_argument("--out", help="results folder (default: output_dir from the YAML)")
    r.add_argument("--broker", default="localhost:1883",
                   help="live only: host:port of Mosquitto, or 'memory' for no broker")
    r.add_argument("--no-mobility", action="store_true", help="live only: devices never leave")
    r.add_argument("--quiet", action="store_true", help="don't print every message")
    r.set_defaults(fn=cmd_run)

    g = sub.add_parser("agent", help="start one live process hosting some nodes")
    g.add_argument("experiment", type=Path)
    g.add_argument("--nodes", default="all",
                   help="all | coord | peers | devices | devices@peer-01 | ids, comma-separated")
    g.add_argument("--broker", default="localhost:1883", help="host:port, or 'memory'")
    g.add_argument("--seed", type=int, default=0)
    g.add_argument("--run-dir", type=Path, default=Path("results/live"))
    g.add_argument("--name", help="agent name (unique per process); default from --nodes")
    g.add_argument("--duration", type=float, help="override duration_s")
    g.add_argument("--ready-timeout", type=float, default=60.0,
                   help="leader: seconds to wait for every node before starting anyway")
    g.add_argument("--no-mobility", action="store_true", help="devices never leave")
    g.add_argument("--quiet", action="store_true", help="don't print every message")
    g.set_defaults(fn=cmd_agent)

    m = sub.add_parser("merge-logs", help="merge a live run's events-*.jsonl")
    m.add_argument("run_dir", type=Path)
    m.set_defaults(fn=cmd_merge_logs)

    t = sub.add_parser("table1", help="compute Table I from a results folder")
    t.add_argument("results")
    t.set_defaults(fn=cmd_table1)

    sub.add_parser("import-legacy", help="(Track 1) legacy XML -> workflow YAML").set_defaults(
        fn=lambda _: (print("`orchflow import-legacy` is Track 1's, not written yet",
                            file=sys.stderr), 2)[1])
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(getattr(args, "quiet", False))
    try:
        return int(args.fn(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
