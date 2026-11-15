"""Standalone Track 2 integration smoke-test while Track 1 builds the real registry.

Run from the orchflow project root after `uv sync --all-extras`:

    uv run python experiments/demo_echo_sim.py --seeds 3 --duration-s 60
    uv run python experiments/demo_echo_sim.py --seeds 100 --duration-s 3600

Uses the contract's tiny_registry() at the requested 20x20 size. Later the Track
3 CLI will call run_simulation with Track 1's actual generated registry.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from orchflow.core.testing import tiny_experiment, tiny_registry, tiny_workflow
from orchflow.metrics.table1 import build_table1, write_table1_csv
from orchflow.runtime.sim.env import run_simulation


def main() -> int:
    parser = argparse.ArgumentParser(description="Run reproducible dummy echo simulations")
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--duration-s", type=float, default=60.0)
    parser.add_argument("--peers", type=int, default=20)
    parser.add_argument("--devices-per-peer", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=Path("results/echo-demo"))
    args = parser.parse_args()
    if min(args.seeds, args.peers, args.devices_per_peer) < 1 or args.duration_s <= 0:
        parser.error("seeds, peers, devices-per-peer and duration must be positive")

    spec = tiny_experiment(name="echo-demo", duration_s=args.duration_s)
    logs: list[Path] = []
    for seed in range(args.seeds):
        path = args.output_dir / f"seed-{seed:03}" / "events.jsonl"
        # Each seed gets a FRESH registry; the simulation updates device availability.
        registry = tiny_registry(n_peers=args.peers, devices_per_peer=args.devices_per_peer)
        result = run_simulation(spec, tiny_workflow(), registry, seed=seed, event_log_path=path)
        logs.append(path)
        print(f"seed {seed}: virtual {result.elapsed_s:g} s, {len(result.events)} events")
    csv_path = args.output_dir / "table1.csv"
    write_table1_csv(csv_path, build_table1(logs))
    print(f"Table I CSV: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
