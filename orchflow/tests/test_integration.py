"""Tracks 1 + 3: the real GOM / LOM logic on the live runtime.

Runs the committed plume workflow with coordinator, peers and devices in three separate
agents on the in-memory broker, with one scripted departure. Timings are scaled down
(period 1 s, heartbeat 0.2 s, fast work) so a run takes a few seconds; the protocol is
unchanged, so the redeployment must cost exactly the legacy 8 (GOM) / 6 (LOM) messages.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from orchflow.core.events import (
    MessageSent,
    RebindEnded,
    TaskInstanceEnded,
    TaskInstanceStarted,
    read_events,
)
from orchflow.core.interfaces import WorkKind
from orchflow.core.models import Artifact, Device, Service
from orchflow.core.registry import build_registry
from orchflow.core.spec import PluginRef, TestbedSpec, WorkflowSpec, load_yaml
from orchflow.core.testing import tiny_experiment
from orchflow.policies.common import task_peer
from orchflow.runtime.live import LiveAgent, LocalBroker, merge_events
from orchflow.runtime.live.agent import select_nodes

ROOT = Path(__file__).resolve().parents[1]
LEAVE_AT = 1.2
FAST = {"heartbeat_s": 0.2, "heartbeat_timeout_s": 0.5, "check_interval_s": 0.25}


class FastWork:
    """Deploy 0.1 s, execute 0.05 s (the real SleepWork takes seconds)."""

    async def run(self, kind: WorkKind, service: Service | None, artifact: Artifact | None,
                  data: dict[str, Any]) -> bool:
        await asyncio.sleep(0.1 if kind == "deploy" else 0.05)
        return True


class LeaveOnce:
    """One chosen device leaves at LEAVE_AT and never comes back."""

    def __init__(self, device_id: str) -> None:
        self.device_id = device_id

    def residence_time_s(self, device: Device, rng: np.random.Generator) -> float:
        return LEAVE_AT if device.id == self.device_id else 1e9

    def rejoin_delay_s(self, device: Device, rng: np.random.Generator) -> float | None:
        return None

    def is_graceful(self, device: Device, rng: np.random.Generator) -> bool:
        return False


@pytest.mark.parametrize(("mode", "cost"), [("gom", 8), ("lom", 6)])
def test_live_redeployment_split_across_agents(tmp_path: Path, mode: str, cost: int) -> None:
    wf = load_yaml(ROOT / "workflows" / "plume.yaml", WorkflowSpec)
    task = wf.tasks[0].model_copy(update={"period_s": 1.0, "deadline_s": 1.0})
    wf = wf.model_copy(update={"tasks": [task]})
    testbed = TestbedSpec(name="it", n_peers=3, devices_per_peer=4, area_center=task.location,
                          services_per_device=[r.service_name for r in task.requirements])
    exp = tiny_experiment(runtime="live", duration_s=6.0,
                          orchestration=PluginRef(name=mode, params=FAST),
                          binding=PluginRef(name="nearest"))

    # the device the plume service (the watched, deployable one) is bound to at start
    reg0 = build_registry(testbed, wf)
    req = next(r for r in task.requirements if r.deployable)
    cell = task_peer(reg0, task).cell_id
    target = min(reg0.candidates(req, task.location, cell_id=cell),
                 key=lambda d: (d.location.distance_m(task.location), d.id)).id

    broker = LocalBroker()

    def agent(nodes: str) -> LiveAgent:
        reg = build_registry(testbed, wf)  # every process builds its own copy
        return LiveAgent(experiment=exp, workflow=wf, registry=reg,
                         nodes=select_nodes(nodes, reg), transport=broker.transport(),
                         run_dir=tmp_path, name=nodes, work=FastWork(),
                         mobility=LeaveOnce(target), ready_timeout_s=5)

    agents = [agent("devices"), agent("peers"), agent("coord")]

    async def scenario() -> None:
        await asyncio.gather(*(a.run() for a in agents))

    asyncio.run(scenario())
    assert sum(a.logic_errors for a in agents) == 0

    events = list(read_events(merge_events(tmp_path)))
    rebind_msgs = [e for e in events if isinstance(e, MessageSent) and e.rebind_id]
    ends = [e for e in events if isinstance(e, RebindEnded)]
    assert len(ends) == 1 and ends[0].ok
    assert len(rebind_msgs) == cost, [m.kind.value for m in rebind_msgs]

    started = [e for e in events if isinstance(e, TaskInstanceStarted)]
    ended = {e.instance: e for e in events if isinstance(e, TaskInstanceEnded)}
    assert len(started) >= 4
    # every instance that had time to finish did, and successfully (late ones may miss)
    assert all(ended[s.instance].ok for s in started if s.instance in ended)
    assert len(started) - len(ended) <= 1
