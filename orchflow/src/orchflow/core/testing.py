"""Fakes and fixtures so every track can build and test ALONE.

SHARED CONTRACT (additions are always welcome -- they don't break anyone).

Track 1 (logic)    : RecordingContext -- a fake NodeContext; drive your GOM/LOM logic by
                     hand and assert on ctx.sent / ctx.events.
Track 2 (sim)      : EchoProtocol ("echo") -- a dummy orchestration protocol that exercises
Track 3 (live)       messages, timers and work, so a runtime can be tested before GOM/LOM
                     exist. Plus tiny_registry / tiny_workflow / tiny_experiment.
Track 2 (metrics)  : sample_events() -- a hand-written event log with KNOWN answers.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from orchflow.core.events import (
    DeviceJoined,
    DeviceLeft,
    Event,
    MessageSent,
    RebindEnded,
    RebindStarted,
    RunEnded,
    RunStarted,
    TaskInstanceEnded,
    TaskInstanceStarted,
)
from orchflow.core.interfaces import NodeContext, NodeLogic, Registry, WorkKind
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import (
    COORDINATOR_ID,
    Artifact,
    Cell,
    Device,
    Location,
    PeerNode,
    Service,
    ServiceKind,
)
from orchflow.core.plugins import register
from orchflow.core.spec import (
    ExperimentSpec,
    PluginRef,
    ServiceRequirement,
    TaskSpec,
    WorkflowSpec,
)

# =========================================================================== registry fake


class StaticRegistry:
    """Minimal in-memory Registry. Track 1's real registry must behave the same way."""

    def __init__(
        self,
        devices: Sequence[Device],
        peers: Sequence[PeerNode],
        cells: Sequence[Cell],
        services: Sequence[Service],
    ) -> None:
        self._devices = {d.id: d for d in devices}
        self._peers = {p.id: p for p in peers}
        self._cells = list(cells)
        self._services: dict[str, list[Service]] = {}
        for s in services:
            self._services.setdefault(s.device_id, []).append(s)
        self._available = {d.id: True for d in devices}

    def devices(self) -> Sequence[Device]:
        return list(self._devices.values())

    def device(self, device_id: str) -> Device:
        return self._devices[device_id]

    def peers(self) -> Sequence[PeerNode]:
        return list(self._peers.values())

    def peer(self, peer_id: str) -> PeerNode:
        return self._peers[peer_id]

    def cells(self) -> Sequence[Cell]:
        return self._cells

    def services_of(self, device_id: str) -> Sequence[Service]:
        return self._services.get(device_id, [])

    def candidates(
        self,
        req: ServiceRequirement,
        near: Location,
        *,
        cell_id: str | None = None,
        exclude: frozenset[str] = frozenset(),
    ) -> Sequence[Device]:
        out = []
        for d in self._devices.values():
            if d.id in exclude or not self._available[d.id]:
                continue
            if cell_id is not None and d.cell_id != cell_id:
                continue
            if not any(s.name == req.service_name for s in self.services_of(d.id)):
                continue
            if d.specs.ram_gb < req.min_ram_gb or d.specs.storage_gb < req.min_storage_gb:
                continue
            if req.radius_m is not None and d.location.distance_m(near) > req.radius_m:
                continue
            out.append(d)
        return out

    def set_available(self, device_id: str, available: bool) -> None:
        self._available[device_id] = available

    def is_available(self, device_id: str) -> bool:
        return self._available[device_id]


CENTER = Location(lat=30.456048, lon=-91.205240)  # IMW workflow location (from legacy XML)
PLUME_JAR = Artifact(name="data_integration_service.jar", size_bytes=2_000_000)


def tiny_registry(n_peers: int = 2, devices_per_peer: int = 3) -> StaticRegistry:
    """n_peers cells side by side; every device offers wind_speed (sensor) and
    plume_service (fog, deployable)."""
    peers, cells, devices, services = [], [], [], []
    k = 0
    for p in range(1, n_peers + 1):
        center = Location(lat=CENTER.lat, lon=CENTER.lon + 0.002 * (p - 1))
        cells.append(Cell(id=f"cell-{p:02d}", center=center, radius_m=100))
        peers.append(PeerNode(id=f"peer-{p:02d}", name=f"peer {p}", cell_id=f"cell-{p:02d}",
                              location=center))
        for _ in range(devices_per_peer):
            k += 1
            dev = Device(id=f"dev-{k:04d}", name=f"device {k}", peer_id=f"peer-{p:02d}",
                         cell_id=f"cell-{p:02d}",
                         location=Location(lat=center.lat + 0.0001 * k, lon=center.lon))
            devices.append(dev)
            services.append(Service(id=f"svc-{k:04d}-ws", name="wind_speed",
                                    kind=ServiceKind.SENSOR, deployable=False,
                                    device_id=dev.id, execution_time_s=0.2))
            services.append(Service(id=f"svc-{k:04d}-pl", name="plume_service",
                                    kind=ServiceKind.FOG, deployable=True, device_id=dev.id,
                                    execution_time_s=2.0, artifact=PLUME_JAR))
    return StaticRegistry(devices, peers, cells, services)


def tiny_workflow() -> WorkflowSpec:
    return WorkflowSpec(
        id="tiny",
        name="tiny plume-like workflow",
        tasks=[
            TaskSpec(id="1", name="read wind", location=CENTER, period_s=60, deadline_s=58,
                     requirements=[ServiceRequirement(service_name="wind_speed",
                                                      kind=ServiceKind.SENSOR)]),
            TaskSpec(id="2", name="plume model", location=CENTER, depends_on=["1"],
                     requirements=[ServiceRequirement(service_name="plume_service",
                                                      kind=ServiceKind.FOG, deployable=True,
                                                      artifact=PLUME_JAR.name)]),
        ],
    )


def tiny_experiment(**overrides: Any) -> ExperimentSpec:
    base: dict[str, Any] = dict(
        name="tiny-echo", workflow=Path("tiny.yaml"), testbed=Path("tiny.yaml"),
        runtime="sim", orchestration=PluginRef(name="echo", params={"period_s": 10}),
        binding=PluginRef(name="first"), duration_s=60, seeds=[0],
    )
    base.update(overrides)
    return ExperimentSpec(**base)


# =========================================================================== context fake


@dataclass
class WorkRequest:
    work_id: str
    kind: WorkKind
    service: Service | None
    artifact: Artifact | None
    data: dict[str, Any]


@dataclass
class RecordingContext:
    """A fake NodeContext that just records what the logic asked for.

        ctx = RecordingContext("coord", tiny_registry(), tiny_experiment(), tiny_workflow())
        logic.on_start(ctx)
        assert [m.kind for m in ctx.sent] == [...]
        ctx.fire_timer(logic, "heartbeat-timeout")      # jumps the clock to the timer

    Nothing is delivered anywhere: tests pass messages between logic objects by hand.
    """

    node_id: str
    registry: Registry
    experiment: ExperimentSpec
    workflow: WorkflowSpec
    seed: int = 0
    clock: float = 0.0
    sent: list[Message] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    timers: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)
    work: list[WorkRequest] = field(default_factory=list)
    rng: np.random.Generator = field(init=False)

    def __post_init__(self) -> None:
        self.rng = np.random.default_rng([self.seed, *self.node_id.encode()])

    # --- NodeContext
    def now(self) -> float:
        return self.clock

    def send(self, msg: Message) -> None:
        msg = msg.model_copy(update={"sent_at": self.clock})
        self.sent.append(msg)
        self.events.append(MessageSent(t=self.clock, node=self.node_id, msg_id=msg.id,
                                       kind=msg.kind, src=msg.src, dst=msg.dst,
                                       task_id=msg.task_id, rebind_id=msg.rebind_id))

    def set_timer(self, name: str, delay_s: float, data: dict[str, Any] | None = None) -> None:
        self.timers[name] = (self.clock + delay_s, data or {})

    def cancel_timer(self, name: str) -> None:
        self.timers.pop(name, None)

    def start_work(self, work_id: str, kind: WorkKind, *, service: Service | None = None,
                   artifact: Artifact | None = None, data: dict[str, Any] | None = None) -> None:
        self.work.append(WorkRequest(work_id, kind, service, artifact, data or {}))

    def emit(self, event: Event) -> None:
        self.events.append(event)

    # --- test helpers
    def fire_timer(self, logic: NodeLogic, name: str) -> None:
        due, data = self.timers.pop(name)
        self.clock = max(self.clock, due)
        logic.on_timer(self, name, data)

    def finish_work(self, logic: NodeLogic, work_id: str, ok: bool = True) -> None:
        req = next(w for w in self.work if w.work_id == work_id)
        self.work.remove(req)
        logic.on_work_done(self, work_id, ok, req.data)

    def overhead_messages(self) -> list[Message]:
        return [m for m in self.sent if m.is_overhead]


def _type_check(ctx: RecordingContext) -> NodeContext:
    return ctx  # pyright fails here if RecordingContext stops matching NodeContext


# =========================================================================== dummy plug-ins


@register("binding", "first")
class FirstBinding:
    """Picks the first candidate. For tests only; Track 1 writes the real policies."""

    def select(self, req: Any, candidates: Sequence[Device], near: Any, registry: Any,
               rng: Any) -> Device | None:
        return candidates[0] if candidates else None


class _Quiet:
    """Default no-op callbacks for the echo nodes."""

    def on_start(self, ctx: NodeContext) -> None: ...
    def on_message(self, ctx: NodeContext, msg: Message) -> None: ...
    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None: ...
    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None: ...
    def on_depart(self, ctx: NodeContext, graceful: bool) -> None: ...


class _EchoCoordinator(_Quiet):
    def __init__(self, period_s: float) -> None:
        self.period_s = period_s
        self.instance = 0

    def on_start(self, ctx: NodeContext) -> None:
        ctx.set_timer("tick", 0.0)

    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None:
        self.instance += 1
        ctx.emit(TaskInstanceStarted(t=ctx.now(), node=ctx.node_id, task_id="1",
                                     instance=self.instance,
                                     deadline_at=ctx.now() + self.period_s))
        for peer in ctx.registry.peers():
            ctx.send(Message(kind=MsgKind.TASK_ASSIGN, src=ctx.node_id, dst=peer.id,
                             task_id="1", instance=self.instance))
        ctx.set_timer("tick", self.period_s)

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.TASK_RESULT:
            ctx.emit(TaskInstanceEnded(t=ctx.now(), node=ctx.node_id, task_id="1",
                                       instance=msg.instance or 0, ok=msg.payload["ok"]))


class _EchoPeer(_Quiet):
    def __init__(self, peer: PeerNode) -> None:
        self.peer = peer

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.TASK_ASSIGN:
            devs = [d for d in ctx.registry.devices()
                    if d.peer_id == self.peer.id and ctx.registry.is_available(d.id)]
            if not devs:
                ctx.send(Message(kind=MsgKind.TASK_RESULT, src=ctx.node_id, dst=COORDINATOR_ID,
                                 task_id=msg.task_id, instance=msg.instance,
                                 payload={"ok": False}))
                return
            ctx.send(Message(kind=MsgKind.DATA_REQUEST, src=ctx.node_id, dst=devs[0].id,
                             task_id=msg.task_id, instance=msg.instance))
        elif msg.kind == MsgKind.DATA_REPLY:
            ctx.send(Message(kind=MsgKind.TASK_RESULT, src=ctx.node_id, dst=COORDINATOR_ID,
                             task_id=msg.task_id, instance=msg.instance,
                             payload={"ok": True, "value": msg.payload["value"]}))


class _EchoDevice(_Quiet):
    def __init__(self, device: Device) -> None:
        self.device = device

    def on_start(self, ctx: NodeContext) -> None:
        ctx.send(Message(kind=MsgKind.DEVICE_JOIN, src=ctx.node_id, dst=self.device.peer_id))

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.DATA_REQUEST:
            svc = next((s for s in ctx.registry.services_of(self.device.id)
                        if not s.deployable), None)
            ctx.start_work(f"read-{msg.id}", "execute", service=svc,
                           data={"reply_to": msg.src, "task_id": msg.task_id,
                                 "instance": msg.instance})

    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None:
        ctx.send(Message(kind=MsgKind.DATA_REPLY, src=ctx.node_id, dst=data["reply_to"],
                         task_id=data["task_id"], instance=data["instance"],
                         payload={"value": float(ctx.rng.normal(5.0, 1.0))}))

    def on_depart(self, ctx: NodeContext, graceful: bool) -> None:
        if graceful:
            ctx.send(Message(kind=MsgKind.DEVICE_LEAVE, src=ctx.node_id,
                             dst=self.device.peer_id))


@register("orchestration", "echo")
class EchoProtocol:
    """Dummy protocol for testing RUNTIMES (Tracks 2 and 3) -- not a real orchestration.
    Every ``period_s`` the coordinator sends TASK_ASSIGN to each peer; the peer asks its first
    available device for a reading (DATA_REQUEST -> execute work -> DATA_REPLY) and returns
    TASK_RESULT. With P peers that is exactly 4*P messages per tick (plus DEVICE_JOINs at
    start), which a runtime test can assert on."""

    name = "echo"

    def __init__(self, period_s: float = 10.0) -> None:
        self.period_s = period_s

    def coordinator(self) -> NodeLogic:
        return _EchoCoordinator(self.period_s)

    def peer(self, peer: PeerNode) -> NodeLogic:
        return _EchoPeer(peer)

    def device(self, device: Device) -> NodeLogic:
        return _EchoDevice(device)


# =========================================================================== metrics fixture


def sample_events() -> list[Event]:
    """A tiny hand-written run with KNOWN answers, for testing metrics (Track 2).

    Expected for task "1" (period 60 s, deadline 58 s, 3 instances):
        instances           = 3
        missed deadlines    = 1      (instance 2 ended at t=122 > deadline_at=118)
        delay of the miss   = 4.0 s  (122 - 118)
        rebinds             = 1, redeploy time = 3.0 s (t=115 -> t=118, both on peer-01)
        overhead messages   = 8      (messages carrying rebind_id "rb-1")
        total messages      = 14
    """
    n, c = "coord", "peer-01"
    ev: list[Event] = [
        RunStarted(t=0, node=n, run_id="r0", experiment="sample", runtime="sim",
                   orchestration="gom", seed=0, duration_s=180),
        DeviceJoined(t=0, node=c, device_id="dev-0001", peer_id=c, cell_id="cell-01"),
    ]
    for i, (start, end) in enumerate([(0, 5), (60, 122), (120, 126)], start=1):
        ev.append(TaskInstanceStarted(t=start, node=n, task_id="1", instance=i,
                                      deadline_at=start + 58))
        ev.append(MessageSent(t=start, node=n, msg_id=f"a{i}", kind=MsgKind.TASK_ASSIGN,
                              src=n, dst=c, task_id="1"))
        ev.append(MessageSent(t=end, node=c, msg_id=f"r{i}", kind=MsgKind.TASK_RESULT,
                              src=c, dst=n, task_id="1"))
        ev.append(TaskInstanceEnded(t=end, node=n, task_id="1", instance=i, ok=True))
    ev += [
        DeviceLeft(t=115, node=c, device_id="dev-0001", graceful=False),
        RebindStarted(t=115, node=c, rebind_id="rb-1", task_id="1", service_name="wind_speed",
                      failed_device_id="dev-0001"),
    ]
    for k in range(8):
        ev.append(MessageSent(t=115 + 0.3 * k, node=c, msg_id=f"o{k}",
                              kind=MsgKind.REBIND_REQUEST, src=c, dst=n, task_id="1",
                              rebind_id="rb-1"))
    ev += [
        RebindEnded(t=118, node=c, rebind_id="rb-1", new_device_id="dev-0002", ok=True),
        RunEnded(t=180, node=n, run_id="r0"),
    ]
    return sorted(ev, key=lambda e: e.t)
