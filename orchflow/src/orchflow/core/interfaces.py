"""The plugs between tracks. Read this file first.

SHARED CONTRACT.

The central idea ("sans-I/O")
-----------------------------
GOM/LOM logic (Track 1) is written ONCE as plain, synchronous classes that react to
things happening -- a message arrives, a timer fires, some work finishes -- and respond
by calling methods on a ``NodeContext``. The logic never sleeps, never reads the wall
clock, never opens a socket and never calls ``random``.

Each runtime provides its own NodeContext:

    Track 2 (runtime/sim)  : SimPy virtual clock, in-memory bus, sampled durations
    Track 3 (runtime/live) : asyncio clock, MQTT transport, real deploy/execute

so the SAME logic runs in both, which is what lets us claim the simulator is faithful.
For tests, ``orchflow.core.testing.RecordingContext`` is a fake NodeContext.

    +--------------- node logic (Track 1) ---------------+
    | on_start / on_message / on_timer / on_work_done /   |
    | on_depart                                           |
    +--------------^----------------------+---------------+
        runtime calls these        logic calls ctx.*
    +--------------+----------------------v---------------+
    | NodeContext: now, send, set_timer, cancel_timer,     |
    | start_work, emit, registry, rng, experiment          |
    +-----------------------------------------------------+
       implemented by runtime/sim (Track 2) and runtime/live (Track 3)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, Protocol, runtime_checkable

import numpy as np

from orchflow.core.events import Event
from orchflow.core.messages import Message
from orchflow.core.models import Artifact, Cell, Device, Location, PeerNode, Service
from orchflow.core.spec import ExperimentSpec, ServiceRequirement, WorkflowSpec

WorkKind = Literal["deploy", "execute"]


# =========================================================================== registry
# Implemented by Track 1 (core/registry.py, function build_registry(TestbedSpec)).
# Shared by all nodes in the sim; in the live runtime each process builds its own copy
# from the same TestbedSpec + registry_seed, so they are identical.


@runtime_checkable
class Registry(Protocol):
    def devices(self) -> Sequence[Device]: ...
    def device(self, device_id: str) -> Device: ...
    def peers(self) -> Sequence[PeerNode]: ...
    def peer(self, peer_id: str) -> PeerNode: ...
    def cells(self) -> Sequence[Cell]: ...
    def services_of(self, device_id: str) -> Sequence[Service]: ...

    def candidates(
        self,
        req: ServiceRequirement,
        near: Location,
        *,
        cell_id: str | None = None,  # restrict to one cell (LOM)
        exclude: frozenset[str] = frozenset(),  # device ids that must NOT be returned
    ) -> Sequence[Device]:
        """Available devices offering ``req.service_name`` that meet its hardware minimums
        (and lie within ``req.radius_m`` of ``near`` if set). Order is unspecified;
        choosing among them is the BindingPolicy's job."""
        ...

    def set_available(self, device_id: str, available: bool) -> None: ...
    def is_available(self, device_id: str) -> bool: ...


# =========================================================================== node context
# Implemented by Track 2 (sim) and Track 3 (live). Fake: core/testing.RecordingContext.


class NodeContext(Protocol):
    @property
    def node_id(self) -> str: ...

    @property
    def registry(self) -> Registry: ...

    @property
    def rng(self) -> np.random.Generator:
        """The ONLY source of randomness for logic. Seeded per (run seed, node id)."""
        ...

    @property
    def experiment(self) -> ExperimentSpec: ...

    @property
    def workflow(self) -> WorkflowSpec: ...

    def now(self) -> float:
        """Seconds since run start on THIS node's clock."""
        ...

    def send(self, msg: Message) -> None:
        """Deliver ``msg`` to ``msg.dst``. The runtime sets ``sent_at`` and emits a
        MessageSent event -- logic must NOT emit MessageSent itself. Delivery may be delayed
        (network model) or silently dropped if the destination has departed."""
        ...

    def set_timer(self, name: str, delay_s: float, data: dict[str, Any] | None = None) -> None:
        """Call ``on_timer(ctx, name, data)`` after ``delay_s``. Setting a timer with an
        existing name replaces it (use this for heartbeat timeouts)."""
        ...

    def cancel_timer(self, name: str) -> None: ...

    def start_work(
        self,
        work_id: str,
        kind: WorkKind,
        *,
        service: Service | None = None,
        artifact: Artifact | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Ask the runtime to do something that takes real time: deploy an artifact or
        execute a service. sim: duration sampled from the `work` plug-in. live: really
        transfer the file / run the process. Finishes with
        ``on_work_done(ctx, work_id, ok, data)``."""
        ...

    def emit(self, event: Event) -> None:
        """Append to the event log. Use ``t=ctx.now(), node=ctx.node_id``."""
        ...


# =========================================================================== node logic
# Implemented by Track 1 (policies/gom.py, policies/lom.py).


class NodeLogic(Protocol):
    def on_start(self, ctx: NodeContext) -> None:
        """Node has come up (run start, or a device (re)joining a cell)."""
        ...

    def on_message(self, ctx: NodeContext, msg: Message) -> None: ...

    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None: ...

    def on_work_done(
        self, ctx: NodeContext, work_id: str, ok: bool, data: dict[str, Any]
    ) -> None: ...

    def on_depart(self, ctx: NodeContext, graceful: bool) -> None:
        """DEVICES ONLY. The runtime's mobility model is removing this device. If
        ``graceful`` the device may still send (e.g. DEVICE_LEAVE); after this returns,
        nothing more is delivered to it until it rejoins (on_start again)."""
        ...


class OrchestrationProtocol(Protocol):
    """A complete protocol (GOM or LOM): builds the logic object for every node.
    Registered as plug-in kind "orchestration" under "gom" / "lom"."""

    name: str

    def coordinator(self) -> NodeLogic: ...
    def peer(self, peer: PeerNode) -> NodeLogic: ...
    def device(self, device: Device) -> NodeLogic: ...


# =========================================================================== policies
# Pure functions of their inputs; registered as plug-ins and chosen in experiment YAML.


class BindingPolicy(Protocol):
    """kind "binding". Track 1: "nearest" (paper behaviour), "qos"."""

    def select(
        self,
        req: ServiceRequirement,
        candidates: Sequence[Device],
        near: Location,
        registry: Registry,
        rng: np.random.Generator,
    ) -> Device | None: ...


class MobilityModel(Protocol):
    """kind "mobility". Track 2: "exponential" (mean 5 min). Used by BOTH runtimes to
    decide when devices leave (the live runtime enforces it by stopping the agent)."""

    def residence_time_s(self, device: Device, rng: np.random.Generator) -> float: ...

    def rejoin_delay_s(self, device: Device, rng: np.random.Generator) -> float | None:
        """Seconds until the device (or a replacement) re-enters a cell; None = never."""
        ...

    def is_graceful(self, device: Device, rng: np.random.Generator) -> bool: ...


class NetworkModel(Protocol):
    """kind "network". SIM ONLY (live uses real links shaped by tc netem). Track 2:
    "ideal" (0 s), "lan", "edge_wifi"."""

    def latency_s(self, msg: Message, rng: np.random.Generator) -> float: ...


class WorkModel(Protocol):
    """kind "work". SIM ONLY. How long deploy / execute take. Track 2: "fixed", "sampled"."""

    def duration_s(
        self,
        kind: WorkKind,
        service: Service | None,
        artifact: Artifact | None,
        rng: np.random.Generator,
    ) -> float: ...


class EventSink(Protocol):
    def emit(self, event: Event) -> None: ...
