"""deploy/vm/orchflow_vm.py: inventory parsing, node split and the commands it would run.

Nothing here touches SSH or Docker; `--dry-run` only prints commands.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
VM = ROOT / "deploy" / "vm"
EXPERIMENT = ROOT / "experiments" / "plume_gom.yaml"


def _load():
    spec = importlib.util.spec_from_file_location("orchflow_vm", VM / "orchflow_vm.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclasses look the module up here
    spec.loader.exec_module(mod)
    return mod


vm = _load()


def _inventory(tmp_path: Path, hosts: str, extra: str = "") -> Path:
    p = tmp_path / "inventory.yaml"
    p.write_text(f"{extra}hosts:\n{hosts}", encoding="utf-8")
    return p


def test_expand_peers() -> None:
    assert vm.expand_peers("1-3") == ["peer-01", "peer-02", "peer-03"]
    assert vm.expand_peers("2,5-6") == ["peer-02", "peer-05", "peer-06"]
    assert vm.expand_peers(7) == ["peer-07"]
    for bad in ("0-2", "3-1", "x"):
        with pytest.raises(ValueError):
            vm.expand_peers(bad)


def test_example_inventory_covers_paper_testbed() -> None:
    inv = vm.load_inventory(VM / "inventory.example.yaml")
    assert inv.broker == "orchflow-vm-1"
    assert inv.hosts[1].nodes.startswith("peer-01,devices@peer-01,peer-02")
    placement, problems = vm.node_split(inv, EXPERIMENT)
    assert problems == []
    assert placement["coord"] == "orchflow-vm-1"
    assert placement["peer-20"] == "orchflow-vm-3"
    assert len(placement) == 1 + 20 + 400


def test_node_split_reports_gaps_and_overlaps(tmp_path: Path) -> None:
    inv = vm.load_inventory(_inventory(tmp_path, (
        "  a: {nodes: coord, run_broker: true}\n"
        "  b: {peers: 1-10}\n"
        "  c: {peers: 10-19}\n"  # peer-10 twice, peer-20 nowhere
    )))
    _, problems = vm.node_split(inv, EXPERIMENT)
    assert any("peer-10 is on both b and c" in p for p in problems)
    assert any("node(s) on no host" in p and "peer-20" in p for p in problems)


def test_inventory_needs_a_broker(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="broker"):
        vm.load_inventory(_inventory(tmp_path, "  a: {nodes: coord}\n"))


def test_agent_command() -> None:
    inv = vm.load_inventory(VM / "inventory.example.yaml")
    host = vm.Host(name="orchflow-vm-2", nodes="peer-01,devices@peer-01", cpus=1.5)
    cmd = vm.agent_command(inv, host, "experiments/plume_gom.yaml", seed=4,
                           run_dir="plume-gom-live-x/seed-004", duration=120.0,
                           ready_timeout=90.0, no_mobility=False)
    assert "docker run -d --name orchflow-agent --network host" in cmd
    assert "--cpus 1.5" in cmd
    assert "--broker orchflow-vm-1:1883" in cmd
    assert "--seed 4 --run-dir /results/plume-gom-live-x/seed-004" in cmd
    assert "--duration 120.0" in cmd
    assert "--no-mobility" not in cmd


def test_run_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    code = vm.main(["-i", str(VM / "inventory.example.yaml"), "--dry-run", "run",
                    str(EXPERIMENT), "--seeds", "1", "--duration", "60"])
    out = capsys.readouterr().out
    assert code == 0
    starts = [line for line in out.splitlines() if "docker run" in line]
    assert len(starts) == 3
    assert "orchflow-vm-1" in starts[-1]  # coordinator (leader) starts last
    assert out.count("docker wait orchflow-agent") == 3
    assert out.count("events-orchflow-vm-") == 3  # one log fetched per host


def test_bootstrap_is_valid_bash() -> None:
    bash = subprocess.run(["bash", "--version"], capture_output=True)
    if bash.returncode != 0:
        pytest.skip("bash not available")
    res = subprocess.run(["bash", "-n", str(VM / "bootstrap.sh")], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
