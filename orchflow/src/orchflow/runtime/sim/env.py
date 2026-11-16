"""Fast deterministic SimPy runtime for any Orchflow NodeLogic implementation.

The runtime knows nothing about GOM, LOM, or application-specific tasks. It
translates NodeContext operations into SimPy events and writes the shared event
schema. The same policy objects can therefore run in Track 3's live runtime.

Example (while Track 1's generated registry is under development)::

    from orchflow.core.testing import tiny_experiment, tiny_registry, tiny_workflow
    from orchflow.runtime.sim.env import run_simulation

    result = run_simulation(tiny_experiment(duration_s=3600),
                            tiny_workflow(), tiny_registry(), seed=7)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import simpy

from orchflow.core import plugins
from orchflow.core.events import (
    DeviceJoined,
    DeviceLeft,
    Event,
    EventLogWriter,
    RunEnded,
    RunStarted,
)
from orchflow.core.interfaces import (
    EventSink,
    MobilityModel,
    NetworkModel,
    NodeLogic,
    OrchestrationProtocol,
    Registry,
    WorkKind,
    WorkModel,
)
from orchflow.core.messages import Message
from orchflow.core.models import COORDINATOR_ID, Artifact, Device, Service
from orchflow.core.spec import ExperimentSpec, WorkflowSpec
from orchflow.runtime.sim import mobility as _mobility  # noqa: F401 (register plug-ins)
from orchflow.runtime.sim import network as _network  # noqa: F401
from orchflow.runtime.sim import work as _work  # noqa: F401
from orchflow.runtime.sim.bus import InMemoryBus


@dataclass(frozen=True)
class SimulationResult:
    run_id: str
    seed: int
    elapsed_s: float
    events: tuple[Event, ...]
    event_log_path: Path | None = None


@dataclass
class _NodeState:
    logic: NodeLogic
    context: _SimContext
    is_device: bool = False
    active: bool = True
    lifetime: int = 0
    timer_versions: dict[str, int] = field(default_factory=dict)
    work_versions: dict[str, int] = field(default_factory=dict)


class _SimContext:
    """The NodeContext a policy sees; it exposes no SimPy primitives."""

    def __init__(self, runner: SimRunner, node_id: str) -> None:
        self._runner = runner
        self.node_id = node_id
        self.registry = runner.registry
        self.experiment = runner.experiment
        self.workflow = runner.workflow
        self.rng = runner._rng(f"logic:{node_id}")

    def now(self) -> float:
        return float(self._runner.env.now)

    def send(self, msg: Message) -> None:
        self._runner.bus.send(msg, self.node_id)

    def set_timer(self, name: str, delay_s: float, data: dict[str, Any] | None = None) -> None:
        self._runner.set_timer(self.node_id, name, delay_s, data)

    def cancel_timer(self, name: str) -> None:
        self._runner.cancel_timer(self.node_id, name)

    def start_work(
        self,
        work_id: str,
        kind: WorkKind,
        *,
        service: Service | None = None,
        artifact: Artifact | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self._runner.start_work(self.node_id, work_id, kind, service, artifact, data)

    def emit(self, event: Event) -> None:
        if event.node != self.node_id:
            raise ValueError(f"node {self.node_id} emitted an event for {event.node}")
        self._runner.emit(event)


def _valid_delay(value: float, description: str) -> float:
    delay = float(value)
    if not np.isfinite(delay) or delay < 0:
        raise ValueError(f"{description} must be finite and nonnegative")
    return delay


class SimRunner:
    def __init__(
        self,
        experiment: ExperimentSpec,
        workflow: WorkflowSpec,
        registry: Registry,
        *,
        seed: int,
        protocol: OrchestrationProtocol | None = None,
        event_log_path: str | Path | None = None,
        enable_mobility: bool = True,
    ) -> None:
        if experiment.runtime != "sim":
            raise ValueError("SimRunner requires experiment.runtime='sim'")
        self.experiment = experiment
        self.workflow = workflow
        self.registry = registry
        self.seed = seed
        self.env = simpy.Environment()
        self.protocol: OrchestrationProtocol = (
            protocol if protocol is not None
            else plugins.create("orchestration", experiment.orchestration)
        )
        self.network: NetworkModel = plugins.create("network", experiment.network)
        self.work_model: WorkModel = plugins.create("work", experiment.work)
        self.mobility: MobilityModel = plugins.create("mobility", experiment.mobility)
        self.enable_mobility = enable_mobility
        self.run_id = f"{experiment.name}-seed-{seed}"
        self.event_log_path = Path(event_log_path) if event_log_path else None
        self._sink: EventSink | None = None
        self.events: list[Event] = []
        self.nodes: dict[str, _NodeState] = {}
        self.devices = {device.id: device for device in registry.devices()}
        self.bus = InMemoryBus(
            self.env, self.network, self._rng("network"), self.emit, self._deliver
        )
        self._has_run = False

    def _rng(self, label: str) -> np.random.Generator:
        # No Python hash() here: its salt changes between processes.
        return np.random.default_rng([self.seed, *label.encode("utf-8")])

    def emit(self, event: Event) -> None:
        self.events.append(event)
        if self._sink is not None:
            self._sink.emit(event)

    def _add_node(self, node_id: str, logic: NodeLogic, *, device: bool = False) -> None:
        if node_id in self.nodes:
            raise ValueError(f"duplicate node id {node_id}")
        ctx = _SimContext(self, node_id)
        self.nodes[node_id] = _NodeState(logic=logic, context=ctx, is_device=device)

    def _deliver(self, msg: Message) -> None:
        recipient = self.nodes.get(msg.dst)
        if recipient is not None and recipient.active:
            recipient.logic.on_message(recipient.context, msg)
        # A delivery to an absent/departed device is silently dropped.
        # The send was still recorded by InMemoryBus.

    def set_timer(
        self, node_id: str, name: str, delay_s: float, data: dict[str, Any] | None
    ) -> None:
        state = self.nodes[node_id]
        delay = _valid_delay(delay_s, "timer delay")
        version = state.timer_versions.get(name, 0) + 1
        state.timer_versions[name] = version
        self.env.process(
            self._timer(node_id, name, version, state.lifetime, delay, dict(data or {}))
        )

    def _timer(self, node_id: str, name: str, version: int, lifetime: int,
               delay_s: float, data: dict[str, Any]):
        yield self.env.timeout(delay_s)
        state = self.nodes[node_id]
        if not state.active or state.lifetime != lifetime:
            return
        if state.timer_versions.get(name) != version:
            return
        state.logic.on_timer(state.context, name, data)

    def cancel_timer(self, node_id: str, name: str) -> None:
        state = self.nodes[node_id]
        state.timer_versions[name] = state.timer_versions.get(name, 0) + 1
        # Incrementing preserves monotonic generations, even if a new timer reuses the name.

    def start_work(
        self,
        node_id: str,
        work_id: str,
        kind: WorkKind,
        service: Service | None,
        artifact: Artifact | None,
        data: dict[str, Any] | None,
    ) -> None:
        state = self.nodes[node_id]
        version = state.work_versions.get(work_id, 0) + 1
        state.work_versions[work_id] = version
        # Separate node-local work RNG stream; logic's RNG consumption cannot change durations.
        rng = self._work_rngs[node_id]
        duration = _valid_delay(
            self.work_model.duration_s(kind, service, artifact, rng), "work duration"
        )
        self.env.process(self._work_done(node_id, work_id, version, state.lifetime,
                                         duration, dict(data or {})))

    def _work_done(self, node_id: str, work_id: str, version: int, lifetime: int,
                   duration_s: float, data: dict[str, Any]):
        yield self.env.timeout(duration_s)
        state = self.nodes[node_id]
        if not state.active or state.lifetime != lifetime:
            return
        if state.work_versions.get(work_id) != version:
            return
        state.logic.on_work_done(state.context, work_id, True, data)

    def _join_device(self, device: Device) -> None:
        state = self.nodes[device.id]
        state.active = True
        state.lifetime += 1
        state.timer_versions.clear()
        state.work_versions.clear()
        self.registry.set_available(device.id, True)
        self.emit(DeviceJoined(t=float(self.env.now), node=device.peer_id,
                               device_id=device.id, peer_id=device.peer_id,
                               cell_id=device.cell_id))
        state.logic.on_start(state.context)

    def _device_lifecycle(self, device: Device):
        rng = self._mobility_rngs[device.id]
        while True:
            delay = _valid_delay(self.mobility.residence_time_s(device, rng),
                                 "residence time")
            yield self.env.timeout(delay)
            state = self.nodes[device.id]
            graceful = self.mobility.is_graceful(device, rng)
            state.logic.on_depart(state.context, graceful)
            state.active = False
            state.lifetime += 1
            state.timer_versions.clear()
            state.work_versions.clear()
            self.registry.set_available(device.id, False)
            self.emit(DeviceLeft(t=float(self.env.now), node=device.peer_id,
                                 device_id=device.id, graceful=graceful))
            rejoin = self.mobility.rejoin_delay_s(device, rng)
            if rejoin is None:
                return
            yield self.env.timeout(_valid_delay(rejoin, "rejoin delay"))
            self._join_device(device)

    def run(self) -> SimulationResult:
        if self._has_run:
            raise RuntimeError("SimRunner instances can only be run once")
        self._has_run = True
        writer = EventLogWriter(self.event_log_path) if self.event_log_path else None
        try:
            self._sink = writer
            self.emit(RunStarted(
                t=0.0, node=COORDINATOR_ID, run_id=self.run_id,
                experiment=self.experiment.name, runtime="sim",
                orchestration=self.protocol.name, seed=self.seed,
                duration_s=self.experiment.duration_s,
            ))
            self._add_node(COORDINATOR_ID, self.protocol.coordinator())
            for peer in self.registry.peers():
                self._add_node(peer.id, self.protocol.peer(peer))
            for device in self.devices.values():
                self._add_node(device.id, self.protocol.device(device), device=True)
            self._work_rngs = {name: self._rng(f"work:{name}") for name in self.nodes}
            self._mobility_rngs = {
                name: self._rng(f"mobility:{name}") for name in self.devices
            }
            self.nodes[COORDINATOR_ID].logic.on_start(self.nodes[COORDINATOR_ID].context)
            for peer in self.registry.peers():
                state = self.nodes[peer.id]
                state.logic.on_start(state.context)
            for device in self.devices.values():
                self._join_device(device)
                if self.enable_mobility:
                    self.env.process(self._device_lifecycle(device))
            self.env.run(until=self.experiment.duration_s)
            self.emit(RunEnded(t=float(self.env.now), node=COORDINATOR_ID,
                               run_id=self.run_id))
        finally:
            self._sink = None
            if writer is not None:
                writer.close()
        return SimulationResult(
            run_id=self.run_id, seed=self.seed, elapsed_s=float(self.env.now),
            events=tuple(self.events), event_log_path=self.event_log_path,
        )


def run_simulation(
    experiment: ExperimentSpec,
    workflow: WorkflowSpec,
    registry: Registry,
    *,
    seed: int = 0,
    protocol: OrchestrationProtocol | None = None,
    event_log_path: str | Path | None = None,
    enable_mobility: bool = True,
) -> SimulationResult:
    """Run a single virtual experiment; no threads, sockets or real sleeping."""
    return SimRunner(
        experiment, workflow, registry, seed=seed, protocol=protocol,
        event_log_path=event_log_path, enable_mobility=enable_mobility,
    ).run()
