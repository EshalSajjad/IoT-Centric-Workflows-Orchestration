"""In-memory transport; **only** the runtime emits MessageSent events."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import simpy

from orchflow.core.events import Event, MessageSent
from orchflow.core.interfaces import NetworkModel
from orchflow.core.messages import Message


class InMemoryBus:
    def __init__(
        self,
        env: simpy.Environment,
        network: NetworkModel,
        rng: np.random.Generator,
        emit: Callable[[Event], None],
        deliver: Callable[[Message], None],
    ) -> None:
        self.env = env
        self.network = network
        self.rng = rng
        self.emit = emit
        self.deliver = deliver
        self.sequence = 0

    def send(self, msg: Message, sender: str) -> None:
        if msg.src != sender:
            raise ValueError(f"{sender} cannot send a message with src={msg.src}")
        self.sequence += 1
        # Pydantic's default Message.id uses uuid4; replace it with a reproducible
        # wire ID. The same ID is used by receivers, results, and the event log.
        transmitted = msg.model_copy(update={
            "id": f"sim-{self.sequence:012d}", "sent_at": float(self.env.now)
        })
        self.emit(MessageSent(
            t=float(self.env.now), node=sender, msg_id=transmitted.id,
            kind=transmitted.kind, src=transmitted.src, dst=transmitted.dst,
            task_id=transmitted.task_id, rebind_id=transmitted.rebind_id,
        ))
        delay = float(self.network.latency_s(transmitted, self.rng))
        if not np.isfinite(delay) or delay < 0:
            raise ValueError("network latency must be finite and nonnegative")
        self.env.process(self._deliver(transmitted, delay))

    def _deliver(self, msg: Message, delay: float):
        yield self.env.timeout(delay)
        self.deliver(msg)
