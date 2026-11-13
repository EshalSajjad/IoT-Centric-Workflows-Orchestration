"""Event log files for live runs.

Each agent process writes its own file, ``events-{agent}.jsonl``, into the shared run
folder (several processes can't safely append to one file). ``merge_events`` combines them
into the single ``events.jsonl`` that metrics (Track 2) read.

Clocks: every agent measures ``t`` from the moment IT received the start signal, so
timestamps from different agents are only roughly aligned. That's fine: the contract says
every duration is computed from two events emitted by the same node.
"""

from __future__ import annotations

from pathlib import Path

from orchflow.core.events import Event, read_events, write_events


class LineEventSink:
    """Like core's EventLogWriter, but flushes every line so a crash loses nothing."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", encoding="utf-8")

    def emit(self, event: Event) -> None:
        self._fh.write(event.model_dump_json() + "\n")
        self._fh.flush()

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()


def merge_events(run_dir: str | Path) -> Path:
    """events-*.jsonl in ``run_dir`` -> one time-sorted ``events.jsonl``. Returns its path."""
    run_dir = Path(run_dir)
    parts = sorted(run_dir.glob("events-*.jsonl"))
    if not parts:
        raise FileNotFoundError(f"no events-*.jsonl files in {run_dir}")
    events: list[Event] = []
    for p in parts:
        events.extend(read_events(p))
    events.sort(key=lambda e: e.t)
    out = run_dir / "events.jsonl"
    write_events(out, events)
    return out
