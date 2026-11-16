"""Track 3: live runtime tests, using the dummy "echo" protocol.

Most tests use the in-memory LocalBroker, so they need no Mosquitto. The last one talks to
a real broker and only runs if ORCHFLOW_TEST_BROKER is set (CI sets it), e.g.
    ORCHFLOW_TEST_BROKER=localhost:1883 uv run pytest -q
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from pathlib import Path

import numpy as np
import pytest

from orchflow.cli import _asyncio_run
from orchflow.cli import main as cli_main
from orchflow.core.events import DeviceLeft, MessageSent, RunStarted, read_events
from orchflow.core.models import Device
from orchflow.core.spec import PluginRef
from orchflow.core.testing import tiny_experiment, tiny_registry, tiny_workflow
from orchflow.runtime.live import LiveAgent, LocalBroker, MqttTransport, merge_events
from orchflow.runtime.live.agent import select_nodes
from orchflow.runtime.live.transport import Transport

ROOT = Path(__file__).resolve().parents[1]
PERIOD = 0.5  # echo tick
# ticks at 0, 0.5, 1.0; each device reply takes 0.2 s, so 1.35 s fits exactly 3 ticks
EXP = tiny_experiment(runtime="live", duration_s=1.35,
                      orchestration=PluginRef(name="echo", params={"period_s": PERIOD}))


def make_agent(nodes: str, transport: Transport, run_dir: Path, name: str,
               registry=None, mobility=None) -> LiveAgent:  # type: ignore[no-untyped-def]
    reg = registry or tiny_registry(2, 3)
    return LiveAgent(experiment=EXP, workflow=tiny_workflow(), registry=reg,
                     nodes=select_nodes(nodes, reg), transport=transport, run_dir=run_dir,
                     name=name, mobility=mobility, ready_timeout_s=5)


def sent_kinds(run_dir: Path) -> Counter[str]:
    events = read_events(merge_events(run_dir))
    return Counter(e.kind.value for e in events if isinstance(e, MessageSent))


def check_echo(kinds: Counter[str], ticks: int = 3, peers: int = 2) -> None:
    """echo: 4 messages per peer per tick, plus one device_join per device."""
    assert kinds["device_join"] == 6
    for k in ("task_assign", "data_request", "data_reply", "task_result"):
        assert kinds[k] == ticks * peers, (k, kinds)


# ------------------------------------------------------------------ in-memory


def test_select_nodes() -> None:
    reg = tiny_registry(2, 3)
    assert select_nodes("coord", reg) == ["coord"]
    assert select_nodes("peers", reg) == ["peer-01", "peer-02"]
    assert select_nodes("devices@peer-02", reg) == ["dev-0004", "dev-0005", "dev-0006"]
    assert len(select_nodes("all", reg)) == 9
    assert select_nodes("coord,coord,peer-01", reg) == ["coord", "peer-01"]
    with pytest.raises(ValueError):
        select_nodes("dev-9999", reg)


def test_echo_one_process(tmp_path: Path) -> None:
    agent = make_agent("all", LocalBroker().transport(), tmp_path, "all")
    asyncio.run(agent.run())
    assert agent.logic_errors == 0
    check_echo(sent_kinds(tmp_path))


def test_echo_split_across_agents_with_start_barrier(tmp_path: Path) -> None:
    """Coordinator starts FIRST and must wait for peers and devices before sending."""
    broker = LocalBroker()

    async def scenario() -> None:
        coord = asyncio.create_task(
            make_agent("coord", broker.transport(), tmp_path, "coord").run())
        await asyncio.sleep(0.3)  # followers join late
        await asyncio.gather(
            coord,
            make_agent("peers", broker.transport(), tmp_path, "peers").run(),
            make_agent("devices", broker.transport(), tmp_path, "devices").run())

    asyncio.run(scenario())
    assert sorted(p.name for p in tmp_path.glob("events-*.jsonl")) == [
        "events-coord.jsonl", "events-devices.jsonl", "events-peers.jsonl"]
    check_echo(sent_kinds(tmp_path))
    events = list(read_events(tmp_path / "events.jsonl"))
    assert sum(isinstance(e, RunStarted) for e in events) == 1


class LeaveOnce:
    """Test mobility: dev-0001 vanishes at t=0.3 s and never returns; others stay."""

    def residence_time_s(self, device: Device, rng: np.random.Generator) -> float:
        return 0.3 if device.id == "dev-0001" else 1e9

    def rejoin_delay_s(self, device: Device, rng: np.random.Generator) -> float | None:
        return None

    def is_graceful(self, device: Device, rng: np.random.Generator) -> bool:
        return True


def test_departure_is_seen_by_other_agents(tmp_path: Path) -> None:
    """dev-0001 leaves in the devices process; the peers process (its own registry copy)
    must stop picking it, so peer-01's later requests go to dev-0002."""
    broker = LocalBroker()

    async def scenario() -> None:
        await asyncio.gather(
            make_agent("coord", broker.transport(), tmp_path, "coord").run(),
            make_agent("peers", broker.transport(), tmp_path, "peers").run(),
            make_agent("devices", broker.transport(), tmp_path, "devices",
                       mobility=LeaveOnce()).run())

    asyncio.run(scenario())
    events = list(read_events(merge_events(tmp_path)))
    left = [e for e in events if isinstance(e, DeviceLeft)]
    assert [(e.device_id, e.graceful) for e in left] == [("dev-0001", True)]
    reqs = [e for e in events if isinstance(e, MessageSent) and e.kind == "data_request"
            and e.src == "peer-01"]
    assert [e.dst for e in reqs] == ["dev-0001", "dev-0002", "dev-0002"]
    leaves = [e for e in events if isinstance(e, MessageSent) and e.kind == "device_leave"]
    assert len(leaves) == 1  # graceful departure: echo device says goodbye


def test_cli_run_memory(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    exp = ROOT / "deploy/local/experiment_echo.yaml"
    assert cli_main(["run", str(exp), "--broker", "memory", "--duration", "0.5",
                     "--out", str(tmp_path), "--quiet"]) == 0
    assert list(tmp_path.rglob("events.jsonl"))


# ------------------------------------------------------------------ real Mosquitto


@pytest.mark.skipif(not os.environ.get("ORCHFLOW_TEST_BROKER"),
                    reason="set ORCHFLOW_TEST_BROKER=host:port to test against Mosquitto")
def test_echo_over_real_mqtt(tmp_path: Path) -> None:
    host, _, port = os.environ["ORCHFLOW_TEST_BROKER"].partition(":")

    def mqtt(name: str) -> MqttTransport:
        return MqttTransport(host, int(port or 1883), client_id=f"test-{name}-{os.getpid()}")

    async def scenario() -> None:
        await asyncio.gather(
            make_agent("coord", mqtt("coord"), tmp_path, "coord").run(),
            make_agent("peers", mqtt("peers"), tmp_path, "peers").run(),
            make_agent("devices", mqtt("devices"), tmp_path, "devices").run())

    _asyncio_run(scenario())
    check_echo(sent_kinds(tmp_path))
