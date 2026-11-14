"""A tiny deterministic message network for Track 1's tests.

Wires GOM/LOM node logic together so a whole redeployment can be tested end to end:
messages are delivered after ``latency_s``, timers fire in time order, work takes a fixed
time, and a departed device silently stops receiving anything. It is deliberately minimal
-- the real simulator is Track 2's runtime/sim; this only exists so the logic can be
tested on its own.
"""

from __future__ import annotations

import heapq
from typing import Any

import numpy as np

from orchflow.core.events import DeviceLeft, Event, MessageSent
from orchflow.core.interfaces import NodeLogic, OrchestrationProtocol, Registry, WorkKind
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import COORDINATOR_ID, Artifact, Service
from orchflow.core.spec import ExperimentSpec, WorkflowSpec


class NetContext:
    def __init__(self, net: Net, node_id: str) -> None:
        self.net, self.node_id = net, node_id
        self.rng = np.random.default_rng([net.seed, *node_id.encode()])

    @property
    def registry(self) -> Registry:
        return self.net.registry

    @property
    def experiment(self) -> ExperimentSpec:
        return self.net.experiment

    @property
    def workflow(self) -> WorkflowSpec:
        return self.net.workflow

    def now(self) -> float:
        return self.net.t

    def send(self, msg: Message) -> None:
        msg = msg.model_copy(update={"sent_at": self.net.t})
        self.net.sent.append(msg)
        self.net.events.append(MessageSent(t=self.net.t, node=self.node_id, msg_id=msg.id,
                                           kind=msg.kind, src=msg.src, dst=msg.dst,
                                           task_id=msg.task_id, rebind_id=msg.rebind_id))
        self.net.push(self.net.t + self.net.latency_s, "deliver", msg.dst, msg)

    def set_timer(self, name: str, delay_s: float, data: dict[str, Any] | None = None) -> None:
        token = self.net.next_seq()
        self.net.timers[(self.node_id, name)] = token
        self.net.push(self.net.t + delay_s, "timer", self.node_id, (name, data or {}, token))

    def cancel_timer(self, name: str) -> None:
        self.net.timers.pop((self.node_id, name), None)

    def start_work(self, work_id: str, kind: WorkKind, *, service: Service | None = None,
                   artifact: Artifact | None = None, data: dict[str, Any] | None = None) -> None:
        dur = self.net.deploy_s if kind == "deploy" else self.net.execute_s
        self.net.work.append((self.node_id, work_id, kind))
        self.net.push(self.net.t + dur, "work", self.node_id, (work_id, data or {}))

    def emit(self, event: Event) -> None:
        self.net.events.append(event)


class Net:
    def __init__(self, protocol: OrchestrationProtocol, registry: Registry,
                 workflow: WorkflowSpec, experiment: ExperimentSpec, *, seed: int = 0,
                 latency_s: float = 0.01, deploy_s: float = 3.0, execute_s: float = 1.0) -> None:
        self.registry, self.workflow, self.experiment = registry, workflow, experiment
        self.seed, self.latency_s, self.deploy_s, self.execute_s = (
            seed, latency_s, deploy_s, execute_s)
        self.t = 0.0
        self._q: list[tuple[float, int, str, str, Any]] = []
        self._seq = 0
        self.timers: dict[tuple[str, str], int] = {}
        self.sent: list[Message] = []
        self.events: list[Event] = []
        self.work: list[tuple[str, str, str]] = []
        self.gone: set[str] = set()
        self.logic: dict[str, NodeLogic] = {COORDINATOR_ID: protocol.coordinator()}
        for p in registry.peers():
            self.logic[p.id] = protocol.peer(p)
        for d in registry.devices():
            self.logic[d.id] = protocol.device(d)
        self.ctx = {n: NetContext(self, n) for n in self.logic}
        for n in [*(d.id for d in registry.devices()), *(p.id for p in registry.peers()),
                  COORDINATOR_ID]:
            self.logic[n].on_start(self.ctx[n])

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def push(self, t: float, kind: str, node: str, item: Any) -> None:
        heapq.heappush(self._q, (t, self.next_seq(), kind, node, item))

    def depart(self, device_id: str, at: float) -> None:
        self.push(at, "depart", device_id, None)

    def run(self, until: float) -> None:
        while self._q and self._q[0][0] <= until:
            t, _, kind, node, item = heapq.heappop(self._q)
            self.t = t
            if node in self.gone:
                continue
            logic, ctx = self.logic.get(node), self.ctx.get(node)
            if logic is None or ctx is None:
                continue
            if kind == "deliver":
                logic.on_message(ctx, item)
            elif kind == "timer":
                name, data, token = item
                if self.timers.get((node, name)) == token:
                    del self.timers[(node, name)]
                    logic.on_timer(ctx, name, data)
            elif kind == "work":
                work_id, data = item
                logic.on_work_done(ctx, work_id, True, data)
            elif kind == "depart":
                self.gone.add(node)
                self.events.append(DeviceLeft(t=t, node=node, device_id=node, graceful=False))
                logic.on_depart(ctx, False)
        self.t = until

    # --- inspection helpers
    def of_type(self, cls: type) -> list[Any]:
        return [e for e in self.events if isinstance(e, cls)]

    def rebind_messages(self, rebind_id: str) -> list[Message]:
        return [m for m in self.sent if m.rebind_id == rebind_id]

    def kinds(self, msgs: list[Message]) -> list[MsgKind]:
        return [m.kind for m in msgs]
