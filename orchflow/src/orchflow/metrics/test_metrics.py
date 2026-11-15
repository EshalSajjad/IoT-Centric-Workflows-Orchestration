"""Track 2 metrics tests: the documented sample is the source of truth."""

from __future__ import annotations

import pytest

from orchflow.core.events import TaskInstanceEnded, write_events
from orchflow.core.testing import sample_events
from orchflow.metrics.compute import compute_from_file, compute_metrics
from orchflow.metrics.table1 import build_table1, estimate_95, write_table1_csv


def test_sample_events_match_documented_table1_answers(tmp_path):
    metrics = compute_metrics(sample_events())
    assert metrics.instances == 3
    assert metrics.missed_deadlines == 1
    assert metrics.mean_miss_delay_s == pytest.approx(4.0)
    assert metrics.rebinds == 1
    assert metrics.mean_redeploy_s == pytest.approx(3.0)
    assert metrics.overhead_messages == 8
    assert metrics.total_messages == 14
    event_path = tmp_path / "events.jsonl"
    write_events(event_path, sample_events())
    assert compute_from_file(event_path) == metrics


def test_all_results_for_one_task_instance_count_once():
    ev = sample_events()
    end = next(e for e in ev if isinstance(e, TaskInstanceEnded) and e.instance == 1)
    ev.append(end.model_copy(update={"t": end.t + 1.0}))
    assert compute_metrics(ev).instances == 3


def test_measured_tasks_filter_messages_and_runs():
    sample = sample_events()
    actual = compute_metrics(sample, measured_tasks=["1"])
    assert actual.overhead_messages == 8
    assert actual.total_messages == 14
    empty = compute_metrics(sample, measured_tasks=["other"])
    assert empty.instances == 0
    assert empty.total_messages == 0


def test_ci_uses_student_t_across_seeds():
    ci = estimate_95([1, 2, 3])
    assert ci.n == 3 and ci.mean == 2
    assert ci.lower_95 == pytest.approx(-0.4841377)
    assert ci.upper_95 == pytest.approx(4.4841377)
    assert estimate_95([4]).lower_95 is None


def test_table1_from_files(tmp_path):
    original = sample_events()
    paths = []
    for i in range(3):
        cloned = [e.model_copy(update={"seed": i}) if e.type == "run_started" else e
                  for e in original]
        path = tmp_path / f"seed-{i}" / "events.jsonl"
        write_events(path, cloned)
        paths.append(path)
    rows = build_table1(paths)
    overhead = next(row for row in rows if row.metric == "overhead_messages")
    assert overhead.mean == 8 and overhead.n == 3
    assert overhead.lower_95 == overhead.upper_95 == 8
    out = tmp_path / "table.csv"
    write_table1_csv(out, rows)
    assert "overhead_messages" in out.read_text()
