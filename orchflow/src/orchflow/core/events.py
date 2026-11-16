"""Event log: everything that happens in a run, one JSON object per line (JSONL).

SHARED CONTRACT. Metrics (Track 2) are computed ONLY from this log, never from stdout,
so this schema is what connects the runtimes to the Table I numbers.

Who emits what
--------------
runtime (sim + live) : RunStarted, RunEnded, MessageSent (automatically, on every send),
                       DeviceJoined, DeviceLeft (mobility is driven by the runtime)
node logic (Track 1) : TaskInstanceStarted, TaskInstanceEnded, DeviceBound,
                       RebindStarted, RebindEnded, DeploymentStarted, DeploymentEnded
                       -- via ctx.emit(...)

Time
----
``t`` is ALWAYS ``ctx.now()`` in seconds since run start, taken on the emitting node.
Never subtract timestamps emitted by different machines (live runtime): every duration
metric must be computable from events emitted by ONE node (see docs/contracts.md).

Versioning
----------
Bump SCHEMA_VERSION on any breaking change, and keep read_events() able to read the
previous version, so old result logs stay usable.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from orchflow.core.messages import MsgKind

SCHEMA_VERSION = 1


class _Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    v: int = SCHEMA_VERSION
    t: float  # seconds since run start, from ctx.now()
    node: str  # node that emitted the event


# ---------------------------------------------------------------- run


class RunStarted(_Event):
    type: Literal["run_started"] = "run_started"
    run_id: str
    experiment: str
    runtime: Literal["sim", "live"]
    orchestration: str  # "gom" | "lom"
    seed: int
    duration_s: float


class RunEnded(_Event):
    type: Literal["run_ended"] = "run_ended"
    run_id: str


# ---------------------------------------------------------------- tasks


class TaskInstanceStarted(_Event):
    type: Literal["task_instance_started"] = "task_instance_started"
    task_id: str
    instance: int
    deadline_at: float | None  # absolute t of the deadline, None if the task has none


class TaskInstanceEnded(_Event):
    """Missed deadline / delay are DERIVED by metrics: missed = not ok or t > deadline_at."""

    type: Literal["task_instance_ended"] = "task_instance_ended"
    task_id: str
    instance: int
    ok: bool


# ---------------------------------------------------------------- devices


class DeviceJoined(_Event):
    type: Literal["device_joined"] = "device_joined"
    device_id: str
    peer_id: str
    cell_id: str


class DeviceLeft(_Event):
    type: Literal["device_left"] = "device_left"
    device_id: str
    graceful: bool  # True = announced departure; False = vanished (crash / out of range)


class DeviceBound(_Event):
    type: Literal["device_bound"] = "device_bound"
    task_id: str
    service_name: str
    device_id: str
    rebind_id: str | None = None  # set if this binding is the result of a rebind


# ---------------------------------------------------------------- redeployment


class RebindStarted(_Event):
    """Emitted by the node that detects the failure. Redeploy time =
    RebindEnded.t - RebindStarted.t, both emitted by the SAME node."""

    type: Literal["rebind_started"] = "rebind_started"
    rebind_id: str
    task_id: str
    service_name: str
    failed_device_id: str


class RebindEnded(_Event):
    type: Literal["rebind_ended"] = "rebind_ended"
    rebind_id: str
    new_device_id: str | None  # None = no replacement found
    ok: bool


class DeploymentStarted(_Event):
    type: Literal["deployment_started"] = "deployment_started"
    device_id: str
    artifact: str
    rebind_id: str | None = None


class DeploymentEnded(_Event):
    type: Literal["deployment_ended"] = "deployment_ended"
    device_id: str
    artifact: str
    ok: bool
    rebind_id: str | None = None


# ---------------------------------------------------------------- messages


class MessageSent(_Event):
    """Emitted by the RUNTIME for every Message, never by node logic."""

    type: Literal["message_sent"] = "message_sent"
    msg_id: str
    kind: MsgKind
    src: str
    dst: str
    task_id: str | None = None
    rebind_id: str | None = None


Event = Annotated[
    RunStarted
    | RunEnded
    | TaskInstanceStarted
    | TaskInstanceEnded
    | DeviceJoined
    | DeviceLeft
    | DeviceBound
    | RebindStarted
    | RebindEnded
    | DeploymentStarted
    | DeploymentEnded
    | MessageSent,
    Field(discriminator="type"),
]

_adapter: TypeAdapter[Event] = TypeAdapter(Event)


def parse_event(raw: str | bytes | dict) -> Event:
    if isinstance(raw, dict):
        return _adapter.validate_python(raw)
    return _adapter.validate_json(raw)


class EventLogWriter:
    """Append-only JSONL writer. Runtimes use this as their EventSink."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", encoding="utf-8")

    def emit(self, event: Event) -> None:
        self._fh.write(event.model_dump_json() + "\n")

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> EventLogWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def read_events(path: str | Path) -> Iterator[Event]:
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield parse_event(line)


def write_events(path: str | Path, events: Iterable[Event]) -> None:
    with EventLogWriter(path) as w:
        for e in events:
            w.emit(e)
