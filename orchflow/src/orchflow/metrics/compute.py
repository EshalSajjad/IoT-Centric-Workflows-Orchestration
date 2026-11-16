"""Derive run-level metrics solely from the shared Event log.

No node logic, runtime state, stdout, or database access is required. The same
metrics work on JSONL produced by the SimPy and MQTT runtimes.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from orchflow.core.events import (
    Event,
    MessageSent,
    RebindEnded,
    RebindStarted,
    RunEnded,
    RunStarted,
    TaskInstanceEnded,
    TaskInstanceStarted,
    read_events,
)


@dataclass(frozen=True)
class RunMetrics:
    run_id: str
    experiment: str
    orchestration: str
    seed: int
    instances: int
    missed_deadlines: int
    missed_deadline_pct: float
    mean_miss_delay_s: float | None
    rebinds: int
    completed_rebinds: int
    mean_redeploy_s: float | None
    overhead_messages: int
    overhead_per_rebind: float | None
    total_messages: int


def compute_metrics(
    events: Iterable[Event], measured_tasks: Iterable[str] | None = None
) -> RunMetrics:
    """Compute one run's summary.

    A failed or overdue task misses its deadline. Missing completions count as
    misses only if a deadline is known to have expired by run end. Delay is the
    lateness beyond the deadline *among misses*, not execution time.
    Redeploy durations are paired by rebind_id AND emitting node (the live clocks
    need not be synchronized). Only successful rebinds contribute time samples.
    Every send with a rebind_id contributes one overhead message.

    When measured_tasks is provided, messages are filtered by task_id or by
    rebind_id associated with a measured task. Other untagged traffic is omitted.
    """
    log = list(events)
    starts = [e for e in log if isinstance(e, RunStarted)]
    if len(starts) != 1:
        raise ValueError("expected exactly one RunStarted event per run")
    run = starts[0]
    ends = [e for e in log if isinstance(e, RunEnded)]
    if len(ends) > 1:
        raise ValueError("expected at most one RunEnded event per run")
    end_time = ends[0].t if ends else run.duration_s
    measured = set(measured_tasks) if measured_tasks is not None else None
    def included(task_id: str) -> bool:
        return measured is None or task_id in measured

    opened: dict[tuple[str, str, int], TaskInstanceStarted] = {}
    finished: dict[tuple[str, str, int], list[TaskInstanceEnded]] = {}
    rebind_starts: dict[str, RebindStarted] = {}
    rebind_ends: dict[str, RebindEnded] = {}

    for event in log:
        if isinstance(event, TaskInstanceStarted) and included(event.task_id):
            key = event.node, event.task_id, event.instance
            if key in opened:
                raise ValueError(f"duplicate task instance start: {key}")
            opened[key] = event
        elif isinstance(event, TaskInstanceEnded) and included(event.task_id):
            key = event.node, event.task_id, event.instance
            # A fan-out protocol may issue several results for one task instance.
            # Count it once, finish at the latest result, and require all to succeed.
            finished.setdefault(key, []).append(event)
        elif isinstance(event, RebindStarted) and included(event.task_id):
            if event.rebind_id in rebind_starts:
                raise ValueError(f"duplicate rebind_id: {event.rebind_id}")
            rebind_starts[event.rebind_id] = event
        elif isinstance(event, RebindEnded):
            rebind_ends[event.rebind_id] = event

    missed, miss_delays = 0, []
    for key, start in opened.items():
        results = finished.get(key)
        if not results:
            if start.deadline_at is not None and start.deadline_at <= end_time:
                missed += 1
                miss_delays.append(max(0.0, end_time - start.deadline_at))
            continue
        if any(item.t < start.t for item in results):
            raise ValueError(f"task {key} ended before it started")
        last_end = max(item.t for item in results)
        late_s = max(0.0, last_end - start.deadline_at) if start.deadline_at is not None else 0.0
        if not all(item.ok for item in results) or late_s > 0:
            missed += 1
            miss_delays.append(late_s)

    redeploy_durations = []
    for rebind_id, start in rebind_starts.items():
        finish = rebind_ends.get(rebind_id)
        if finish is None or not finish.ok:
            continue
        if finish.node != start.node:
            raise ValueError(f"rebind {rebind_id} spans different nodes/clocks")
        duration = finish.t - start.t
        if duration < 0:
            raise ValueError(f"rebind {rebind_id} ended before it started")
        redeploy_durations.append(duration)

    messages = [e for e in log if isinstance(e, MessageSent)]
    if measured is not None:
        messages = [
            m for m in messages
            if m.task_id in measured or m.rebind_id in rebind_starts
        ]
    overhead = sum(m.rebind_id is not None for m in messages)
    n_rebinds = len(rebind_starts)
    return RunMetrics(
        run_id=run.run_id,
        experiment=run.experiment,
        orchestration=run.orchestration,
        seed=run.seed,
        instances=len(opened),
        missed_deadlines=missed,
        missed_deadline_pct=100.0 * missed / len(opened) if opened else 0.0,
        mean_miss_delay_s=(sum(miss_delays) / len(miss_delays) if miss_delays else None),
        rebinds=n_rebinds,
        completed_rebinds=len(redeploy_durations),
        mean_redeploy_s=(sum(redeploy_durations) / len(redeploy_durations)
                         if redeploy_durations else None),
        overhead_messages=overhead,
        overhead_per_rebind=overhead / n_rebinds if n_rebinds else None,
        total_messages=len(messages),
    )


def compute_from_file(
    path: str | Path, measured_tasks: Iterable[str] | None = None
) -> RunMetrics:
    """Read a JSONL file and compute metrics with no in-memory simulation needed."""
    return compute_metrics(read_events(path), measured_tasks=measured_tasks)
