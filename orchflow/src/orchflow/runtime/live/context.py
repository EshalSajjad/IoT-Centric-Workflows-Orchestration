"""LiveContext: the real-world ``ctx`` handed to node logic (GOM / LOM / echo).

It implements orchflow.core.interfaces.NodeContext:

    now()          real seconds since this agent got the start signal
    send(msg)      log MessageSent, then publish to orchflow/{dst}/in over MQTT
    set_timer()    asyncio call_later; same name replaces the old timer
    start_work()   run deploy/execute in the background, then call on_work_done
    emit(event)    append to this agent's events-*.jsonl
    rng            numpy Generator seeded from (run seed, node id), same as the fakes

Logic is synchronous and never awaits, so send() can't publish directly: it puts the
message on the agent's outbox queue and a single sender task publishes in order.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import numpy as np

from orchflow.core.events import Event, MessageSent
from orchflow.core.interfaces import NodeLogic, Registry, WorkKind
from orchflow.core.messages import Message
from orchflow.core.models import Artifact, Service
from orchflow.core.spec import ExperimentSpec, WorkflowSpec

if TYPE_CHECKING:
    from orchflow.runtime.live.agent import LiveAgent


class LiveContext:
    def __init__(self, node_id: str, logic: NodeLogic, agent: LiveAgent, seed: int) -> None:
        self._node_id = node_id
        self.logic = logic
        self._agent = agent
        self._rng = np.random.default_rng([seed, *node_id.encode()])
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._work: set[asyncio.Task[None]] = set()
        self.alive = False  # False before start and while a device is away

    # ------------------------------------------------------------------ NodeContext
    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def registry(self) -> Registry:
        return self._agent.registry

    @property
    def rng(self) -> np.random.Generator:
        return self._rng

    @property
    def experiment(self) -> ExperimentSpec:
        return self._agent.experiment

    @property
    def workflow(self) -> WorkflowSpec:
        return self._agent.workflow

    def now(self) -> float:
        return self._agent.now()

    def send(self, msg: Message) -> None:
        t = self.now()
        msg = msg.model_copy(update={"sent_at": t})
        self.emit(MessageSent(t=t, node=self._node_id, msg_id=msg.id, kind=msg.kind,
                              src=msg.src, dst=msg.dst, task_id=msg.task_id,
                              rebind_id=msg.rebind_id))
        self._agent.enqueue(msg)

    def set_timer(self, name: str, delay_s: float, data: dict[str, Any] | None = None) -> None:
        self.cancel_timer(name)
        loop = asyncio.get_running_loop()
        self._timers[name] = loop.call_later(max(0.0, delay_s), self._fire_timer, name,
                                             data or {})

    def cancel_timer(self, name: str) -> None:
        handle = self._timers.pop(name, None)
        if handle is not None:
            handle.cancel()

    def start_work(self, work_id: str, kind: WorkKind, *, service: Service | None = None,
                   artifact: Artifact | None = None, data: dict[str, Any] | None = None) -> None:
        data = data or {}
        task = asyncio.get_running_loop().create_task(
            self._do_work(work_id, kind, service, artifact, data))
        self._work.add(task)
        task.add_done_callback(self._work.discard)

    def emit(self, event: Event) -> None:
        self._agent.sink.emit(event)

    # ------------------------------------------------------------------ runtime side
    def call(self, fn: Callable[..., None], *args: Any) -> None:
        """Call a logic callback; a bug in logic is logged, not fatal to the whole agent."""
        self._agent.call_logic(self._node_id, fn, self, *args)

    def shutdown(self) -> None:
        """Node departs or run ends: no more timers or work completions for it."""
        self.alive = False
        for handle in self._timers.values():
            handle.cancel()
        self._timers.clear()
        for task in list(self._work):
            task.cancel()

    def _fire_timer(self, name: str, data: dict[str, Any]) -> None:
        self._timers.pop(name, None)
        if self.alive:
            self.call(self.logic.on_timer, name, data)

    async def _do_work(self, work_id: str, kind: WorkKind, service: Service | None,
                       artifact: Artifact | None, data: dict[str, Any]) -> None:
        try:
            ok = await self._agent.work.run(kind, service, artifact, data)
        except Exception:
            ok = False
        if self.alive:
            self.call(self.logic.on_work_done, work_id, ok, data)
