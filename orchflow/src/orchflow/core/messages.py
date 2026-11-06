"""Protocol messages: the ONLY way nodes talk to each other, in both runtimes.

SHARED CONTRACT.

Rules
-----
1. Nodes never talk except by ``ctx.send(Message(...))``. The runtime delivers it
   (sim: in-memory bus; live: MQTT topic ``orchflow/{dst}/in``) and logs a MessageSent
   event automatically. That is how message counts are exact.
2. Every message that is part of a redeployment carries the ``rebind_id`` of that
   redeployment. Table I's "messages" column = count of messages with a rebind_id.
   Heartbeats and normal data traffic leave it as None.
3. ``payload`` is a plain JSON-safe dict. Document each kind's payload keys in
   docs/protocol.md (Track 1 owns that file).
4. Track 1 may need more kinds. Add them via a contract PR; never reuse a kind for a
   different meaning.
"""

from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MsgKind(StrEnum):
    # --- task lifecycle (coordinator <-> peer <-> device)
    TASK_ASSIGN = "task_assign"  # coordinator -> peer/device: run this task instance
    TASK_DELEGATE = "task_delegate"  # coordinator -> peer (LOM): you orchestrate this task
    TASK_RESULT = "task_result"  # -> coordinator: task instance output / completion

    # --- binding & deployment
    BIND_REQUEST = "bind_request"  # ask a device to serve a requirement
    BIND_ACK = "bind_ack"
    BIND_NACK = "bind_nack"
    DEPLOY = "deploy"  # transfer an artifact to a device
    DEPLOY_ACK = "deploy_ack"
    START_SERVICE = "start_service"

    # --- data plane
    DATA_REQUEST = "data_request"  # ask a sensor device for a reading
    DATA_REPLY = "data_reply"

    # --- liveness / mobility
    HEARTBEAT = "heartbeat"  # device -> peer ("I am alive")
    DEVICE_JOIN = "device_join"  # device -> peer on entering a cell
    DEVICE_LEAVE = "device_leave"  # device -> peer on graceful departure
    DEVICE_LEFT = "device_left"  # peer -> coordinator: a bound device is gone

    # --- rebinding (redeployment)
    REBIND_REQUEST = "rebind_request"  # ask a node to find a replacement device
    REBIND_REPLY = "rebind_reply"
    PEER_QUERY = "peer_query"  # LOM: ask a neighbouring peer for candidates
    PEER_REPLY = "peer_reply"


class Message(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: MsgKind
    src: str
    dst: str
    payload: dict[str, Any] = {}
    task_id: str | None = None
    instance: int | None = None  # task instance number within the run
    rebind_id: str | None = None  # set on EVERY message belonging to a redeployment
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    sent_at: float | None = None  # filled in by the runtime, not by the sender

    @property
    def is_overhead(self) -> bool:
        """True if this message counts towards Table I communication overhead."""
        return self.rebind_id is not None

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> Message:
        return cls.model_validate_json(raw)
