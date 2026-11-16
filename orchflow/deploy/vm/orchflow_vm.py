"""Run Orchflow's live runtime across VMs, driven from your laptop over SSH.

    python deploy/vm/orchflow_vm.py setup  -i inventory.yaml        # bootstrap every VM
    python deploy/vm/orchflow_vm.py update -i inventory.yaml        # same commit everywhere
    python deploy/vm/orchflow_vm.py check  -i inventory.yaml experiments/plume_gom.yaml
    python deploy/vm/orchflow_vm.py run    -i inventory.yaml experiments/plume_gom.yaml --seeds 3
    python deploy/vm/orchflow_vm.py stop   -i inventory.yaml

Run it from the orchflow/ folder with orchflow installed (`uv sync`), since it checks the
inventory against the experiment's testbed and builds Table I locally. Needs `ssh` and `scp`
on PATH (built into Windows 10+, macOS and Linux). Add --dry-run to print the commands
instead of running them. See deploy/vm/README.md.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ORCHFLOW_ROOT = HERE.parent.parent  # the orchflow/ folder
IMAGE = "orchflow:local"
AGENT = "orchflow-agent"  # container name on every VM
REMOTE_RESULTS = "/var/lib/orchflow/results"
BROKER_PORT = 1883

DRY_RUN = False


# ------------------------------------------------------------------ inventory


@dataclass
class Host:
    name: str  # SSH address (Tailscale name or IP); also the agent's name in the logs
    nodes: str  # value for `orchflow agent --nodes`
    run_broker: bool = False
    cpus: float | None = None  # docker --cpus limit, e.g. 1.0 to look more like a Pi


@dataclass
class Inventory:
    hosts: list[Host]
    broker: str
    ssh_user: str = "ubuntu"
    ssh_key: str | None = None
    ssh_port: int = 22
    repo_dir: str = "/opt/orchflow"
    extra_ssh: list[str] = field(default_factory=list)


def expand_peers(spec: str | int) -> list[str]:
    """'1-10' or '3' or '1-3,7' -> ['peer-01', ..., 'peer-10'] (same ids as the registry)."""
    out: list[str] = []
    for part in str(spec).split(","):
        part = part.strip()
        m = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if not m:
            raise ValueError(f"bad peers range {part!r}; use e.g. 1-10")
        lo, hi = int(m.group(1)), int(m.group(2) or m.group(1))
        if lo < 1 or hi < lo:
            raise ValueError(f"bad peers range {part!r}")
        out += [f"peer-{i:02d}" for i in range(lo, hi + 1)]
    return out


def host_nodes(entry: dict) -> str:
    """`nodes:` as written, plus every `peers:` peer together with its devices."""
    parts: list[str] = []
    if entry.get("nodes"):
        parts.append(str(entry["nodes"]))
    if entry.get("peers") is not None:
        parts += [f"{p},devices@{p}" for p in expand_peers(entry["peers"])]
    if not parts:
        raise ValueError("each host needs `nodes:` and/or `peers:`")
    return ",".join(parts)


def load_inventory(path: Path) -> Inventory:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    hosts_raw = data.get("hosts") or {}
    if not hosts_raw:
        raise ValueError(f"{path}: no hosts")
    hosts = [
        Host(name=name, nodes=host_nodes(entry or {}),
             run_broker=bool((entry or {}).get("run_broker", False)),
             cpus=(entry or {}).get("cpus"))
        for name, entry in hosts_raw.items()
    ]
    brokers = [h.name for h in hosts if h.run_broker]
    broker = data.get("broker") or (brokers[0] if len(brokers) == 1 else None)
    if not broker:
        raise ValueError(f"{path}: set `broker:` or mark exactly one host `run_broker: true`")
    ssh = data.get("ssh") or {}
    key = ssh.get("key")
    return Inventory(
        hosts=hosts, broker=str(broker), ssh_user=ssh.get("user", "ubuntu"),
        ssh_key=os.path.expanduser(key) if key else None, ssh_port=int(ssh.get("port", 22)),
        repo_dir=data.get("repo_dir", "/opt/orchflow"), extra_ssh=list(ssh.get("options", [])),
    )


def node_split(inv: Inventory, experiment: Path) -> tuple[dict[str, str], list[str]]:
    """({node id: host name}, problems) for how the hosts split the experiment's testbed."""
    from orchflow.runtime.live.agent import all_nodes, build_registry, load_inputs, select_nodes

    _, wf, testbed = load_inputs(experiment)
    registry = build_registry(testbed, wf)
    seen: dict[str, str] = {}
    problems: list[str] = []
    for h in inv.hosts:
        try:
            nodes = select_nodes(h.nodes, registry)
        except (ValueError, KeyError) as e:
            problems.append(f"{h.name}: {e}")
            continue
        for n in nodes:
            if n in seen:
                problems.append(f"{n} is on both {seen[n]} and {h.name}")
            seen[n] = h.name
    missing = [n for n in all_nodes(registry) if n not in seen]
    if missing:
        more = f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
        problems.append(f"{len(missing)} node(s) on no host: {', '.join(missing[:5])}{more}")
    return seen, problems


# ------------------------------------------------------------------ ssh helpers


def ssh_base(inv: Inventory, scp: bool = False) -> list[str]:
    cmd = ["scp" if scp else "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    cmd += ["-P" if scp else "-p", str(inv.ssh_port)]
    if inv.ssh_key:
        cmd += ["-i", inv.ssh_key]
    for opt in inv.extra_ssh:
        cmd += ["-o", opt]
    return cmd


def run(cmd: list[str], *, stdin: str | None = None, timeout: float | None = None,
        check: bool = True) -> subprocess.CompletedProcess[str]:
    if DRY_RUN:
        shown = " ".join(shlex.quote(c) for c in cmd)
        print(f"  $ {shown}" + ("  <stdin>" if stdin else ""))
        return subprocess.CompletedProcess(cmd, 0, "", "")
    res = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({res.returncode}): {res.stderr.strip()[-800:]}")
    return res


def remote(inv: Inventory, host: Host, command: str, **kw) -> subprocess.CompletedProcess[str]:
    return run(ssh_base(inv) + [f"{inv.ssh_user}@{host.name}", command], **kw)


def on_all(inv: Inventory, hosts: list[Host], fn) -> dict[str, object]:
    """Run fn(host) on every host in parallel; print and return each result or error."""
    with ThreadPoolExecutor(max_workers=max(1, len(hosts))) as pool:
        futures = {h.name: pool.submit(fn, h) for h in hosts}
    results: dict[str, object] = {}
    for name, fut in futures.items():
        try:
            results[name] = fut.result()
        except Exception as e:  # noqa: BLE001 - report every host, then decide
            results[name] = e
    return results


def report(results: dict[str, object]) -> bool:
    ok = True
    for name, r in results.items():
        if isinstance(r, Exception):
            ok = False
            print(f"  {name}: FAILED  {r}")
        else:
            print(f"  {name}: {r}")
    return ok


def pick(inv: Inventory, only: list[str] | None) -> list[Host]:
    if not only:
        return inv.hosts
    unknown = set(only) - {h.name for h in inv.hosts}
    if unknown:
        raise SystemExit(f"not in the inventory: {', '.join(sorted(unknown))}")
    return [h for h in inv.hosts if h.name in only]


def local_head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ORCHFLOW_ROOT, capture_output=True,
                          text=True, check=True).stdout.strip()


# ------------------------------------------------------------------ commands


def cmd_setup(inv: Inventory, a: argparse.Namespace) -> int:
    """Bootstrap each VM: pipe bootstrap.sh over SSH, settings as exported variables."""
    # strip CRs: a Windows checkout may have CRLF line endings, which bash rejects
    script = (HERE / "bootstrap.sh").read_text(encoding="utf-8").replace(chr(13), "")
    ref = a.ref or "main"

    def one(h: Host) -> str:
        env = {"ORCHFLOW_REF": ref, "ORCHFLOW_USER": inv.ssh_user, "ORCHFLOW_DIR": inv.repo_dir,
               "TS_HOSTNAME": h.name, "RUN_BROKER": "1" if h.run_broker else "0"}
        if os.environ.get("TS_AUTHKEY"):  # secret: read from your shell, never the inventory
            env["TS_AUTHKEY"] = os.environ["TS_AUTHKEY"]
        exports = "".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items())
        # the script goes over stdin, so the auth key never appears on a command line
        res = remote(inv, h, "sudo bash -s", stdin=exports + script, timeout=1800)
        log = Path(a.log_dir) / f"bootstrap-{h.name}.log"
        if not DRY_RUN:
            log.parent.mkdir(parents=True, exist_ok=True)
            log.write_text(res.stdout + res.stderr, encoding="utf-8")
        return f"ok (log: {log})"

    print(f"bootstrapping {len(pick(inv, a.host))} host(s) at ref {ref} ...")
    return 0 if report(on_all(inv, pick(inv, a.host), one)) else 1


def cmd_update(inv: Inventory, a: argparse.Namespace) -> int:
    """Check out one commit on every VM and rebuild the image."""
    ref = a.ref or local_head()
    q = shlex.quote
    command = (f"cd {q(inv.repo_dir)} && git fetch -q origin && "
               f"(git checkout -q --detach {q('origin/' + ref)} 2>/dev/null || "
               f"git checkout -q --detach {q(ref)}) && "
               f"docker build -q -t {IMAGE} -f orchflow/deploy/local/Dockerfile orchflow "
               f">/dev/null && git rev-parse --short HEAD")
    print(f"updating to {ref} (push it first: VMs fetch from GitHub) ...")
    res = on_all(inv, pick(inv, a.host),
                 lambda h: "at " + remote(inv, h, command, timeout=1800).stdout.strip())
    return 0 if report(res) else 1


def cmd_check(inv: Inventory, a: argparse.Namespace) -> int:
    ok = True
    if a.experiment:
        print(f"node split for {a.experiment}:")
        _, problems = node_split(inv, a.experiment)
        for p in problems:
            print(f"  PROBLEM  {p}")
        if not problems:
            print("  every node is on exactly one host")
        ok = not problems
    head = local_head()[:7]
    probe = (f"docker image inspect {IMAGE} >/dev/null 2>&1 "
             "&& echo image=ok || echo image=MISSING; "
             f"git -C {shlex.quote(inv.repo_dir)} rev-parse --short HEAD 2>/dev/null || echo none; "
             f"timeout 3 bash -c '</dev/tcp/{inv.broker}/{BROKER_PORT}' 2>/dev/null "
             f"&& echo broker=ok || echo broker=UNREACHABLE")

    def one(h: Host) -> str:
        lines = remote(inv, h, probe, timeout=30).stdout.split()
        if DRY_RUN:
            return "dry run"
        image, commit, broker = (lines + ["?", "?", "?"])[:3]
        if image != "image=ok" or broker != "broker=ok":
            raise RuntimeError(f"{image} {broker} commit={commit}")
        note = "" if commit == head else f"  (laptop is at {head}: run `update`)"
        return f"{image} {broker} commit={commit}{note}"

    print(f"hosts (broker {inv.broker}:{BROKER_PORT}):")
    ok = report(on_all(inv, inv.hosts, one)) and ok
    return 0 if ok else 1


def agent_command(inv: Inventory, h: Host, experiment: str, seed: int, run_dir: str,
                  duration: float | None, ready_timeout: float, no_mobility: bool) -> str:
    q = shlex.quote
    docker = [f"docker rm -f {AGENT} >/dev/null 2>&1;",
              f"mkdir -p {q(REMOTE_RESULTS)} &&",
              "docker run -d --name", AGENT, "--network host",
              f"-v {q(REMOTE_RESULTS)}:/results"]
    if h.cpus:
        docker.append(f"--cpus {h.cpus}")
    args = ["agent", experiment, "--nodes", h.nodes, "--name", h.name,
            "--broker", f"{inv.broker}:{BROKER_PORT}", "--seed", str(seed),
            "--run-dir", f"/results/{run_dir}", "--ready-timeout", str(ready_timeout), "--quiet"]
    if duration is not None:
        args += ["--duration", str(duration)]
    if no_mobility:
        args.append("--no-mobility")
    return " ".join(docker + [IMAGE] + [q(x) for x in args]) + " >/dev/null"


def cmd_run(inv: Inventory, a: argparse.Namespace) -> int:
    from orchflow.core.models import COORDINATOR_ID
    from orchflow.core.spec import ExperimentSpec, load_yaml

    exp_path = Path(a.experiment)
    exp = load_yaml(exp_path, ExperimentSpec)
    try:  # the containers see the same path relative to /app (= orchflow/)
        exp_in_image = exp_path.resolve().relative_to(ORCHFLOW_ROOT).as_posix()
    except ValueError:
        raise SystemExit(f"{exp_path} must be inside {ORCHFLOW_ROOT} "
                         "(it is baked into the image)") from None
    placement, problems = node_split(inv, exp_path)
    if problems:
        print("the inventory doesn't split the testbed cleanly:\n  " + "\n  ".join(problems))
        return 1

    seeds = list(range(a.seeds)) if a.seeds is not None else exp.seeds
    duration = a.duration if a.duration is not None else exp.duration_s
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = f"{exp.name}-live-{stamp}"
    out = Path(a.out) / base
    coord = [h for h in inv.hosts if h.name == placement[COORDINATOR_ID]]
    followers = [h for h in inv.hosts if h not in coord]
    wait_s = duration + a.ready_timeout + 120

    for seed in seeds:
        run_dir = f"{base}/seed-{seed:03d}"
        print(f"\nseed {seed}: {len(inv.hosts)} hosts, {duration:.0f} s run, results -> {out}")

        def start(h: Host, run_dir: str = run_dir, seed: int = seed) -> str:
            cmd = agent_command(inv, h, exp_in_image, seed, run_dir, a.duration,
                                a.ready_timeout, a.no_mobility)
            remote(inv, h, cmd, timeout=60)
            return "started"

        # followers first, so they are listening when the coordinator (the leader) starts
        if not report(on_all(inv, followers, start)) or not report(on_all(inv, coord, start)):
            cmd_stop(inv, a)
            return 1
        print(f"  waiting up to {wait_s:.0f} s ...")

        def wait(h: Host) -> str:
            code = remote(inv, h, f"docker wait {AGENT}", timeout=wait_s).stdout.strip()
            if DRY_RUN or code == "0":
                return "finished"
            tail = remote(inv, h, f"docker logs --tail 20 {AGENT} 2>&1", check=False).stdout
            raise RuntimeError(f"agent exited with {code}:\n{tail}")

        coord_ok = report(on_all(inv, coord, wait))
        followers_ok = report(on_all(inv, followers, wait))
        ok = coord_ok and followers_ok

        local_dir = out / f"seed-{seed:03d}"
        if not DRY_RUN:
            local_dir.mkdir(parents=True, exist_ok=True)

        def fetch(h: Host, run_dir: str = run_dir, local_dir: Path = local_dir) -> str:
            src = f"{inv.ssh_user}@{h.name}:{REMOTE_RESULTS}/{run_dir}/events-{h.name}.jsonl"
            run(ssh_base(inv, scp=True) + [src, str(local_dir)], timeout=600)
            return "log copied"

        fetched_ok = report(on_all(inv, inv.hosts, fetch))
        ok = ok and fetched_ok
        if not ok:
            print(f"seed {seed} had errors; stopping here. Partial logs: {local_dir}")
            return 1

    if DRY_RUN:
        return 0
    print(f"\nTable I for {out}:")
    return subprocess.run([sys.executable, "-m", "orchflow.cli", "table1", str(out)]).returncode


def cmd_stop(inv: Inventory, a: argparse.Namespace) -> int:
    res = on_all(inv, pick(inv, getattr(a, "host", None)),
                 lambda h: remote(inv, h, f"docker rm -f {AGENT} >/dev/null 2>&1; echo stopped",
                                  timeout=60, check=False).stdout.strip() or "stopped")
    return 0 if report(res) else 1


# ------------------------------------------------------------------ main


def main(argv: list[str] | None = None) -> int:
    global DRY_RUN
    p = argparse.ArgumentParser(prog="orchflow_vm", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-i", "--inventory", type=Path, default=HERE / "inventory.yaml")
    p.add_argument("--dry-run", action="store_true", help="print the commands, run nothing")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("setup", help="bootstrap the VMs (bootstrap.sh over SSH)")
    s.add_argument("--host", action="append", help="only this host (repeatable)")
    s.add_argument("--ref", help="branch/tag/commit to check out (default: main)")
    s.add_argument("--log-dir", default="results/vm-setup", help="where bootstrap logs go")

    u = sub.add_parser("update", help="check out a commit on every VM and rebuild the image")
    u.add_argument("--host", action="append", help="only this host (repeatable)")
    u.add_argument("--ref", help="branch/tag/commit (default: your local HEAD; push it first)")

    c = sub.add_parser("check", help="SSH, image, commit and broker on every VM")
    c.add_argument("experiment", type=Path, nargs="?",
                   help="also check the hosts cover this experiment's testbed")

    r = sub.add_parser("run", help="run an experiment across the VMs and build Table I")
    r.add_argument("experiment", type=Path, help="e.g. experiments/plume_gom.yaml")
    r.add_argument("--seeds", type=int, help="run seeds 0..N-1 (default: from the YAML)")
    r.add_argument("--duration", type=float, help="override duration_s, e.g. 120 for a smoke test")
    r.add_argument("--ready-timeout", type=float, default=120.0,
                   help="how long the coordinator waits for every node to say ready")
    r.add_argument("--no-mobility", action="store_true", help="devices never leave")
    r.add_argument("--out", default="results", help="local results folder")

    st = sub.add_parser("stop", help="remove the agent container on every VM")
    st.add_argument("--host", action="append", help="only this host (repeatable)")

    a = p.parse_args(argv)
    DRY_RUN = a.dry_run
    inv = load_inventory(a.inventory)
    handler = {"setup": cmd_setup, "update": cmd_update, "check": cmd_check, "run": cmd_run,
               "stop": cmd_stop}[a.cmd]
    return handler(inv, a)


if __name__ == "__main__":
    raise SystemExit(main())
