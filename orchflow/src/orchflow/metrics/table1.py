"""Reproduce per-scenario Table I statistics from run event logs.

Each random seed contributes *one run-level observation* to a 95% Student-t
confidence interval. No individual messages are treated as independent samples.

    python -m orchflow.metrics.table1 results/**/*.jsonl -o table1.csv

Or call build_table1([...]) from the Track 3 CLI when it's implemented.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, stdev

from orchflow.metrics.compute import RunMetrics, compute_from_file


@dataclass(frozen=True)
class Estimate:
    n: int
    mean: float | None
    lower_95: float | None
    upper_95: float | None


def estimate_95(values: Iterable[float | int | None]) -> Estimate:
    """95% Student-t CI over independent runs, dropping undefined values."""
    samples = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    n = len(samples)
    if n == 0:
        return Estimate(n=0, mean=None, lower_95=None, upper_95=None)
    average = mean(samples)
    if n == 1:
        return Estimate(n=1, mean=average, lower_95=None, upper_95=None)
    from scipy.stats import t

    half_width = float(t.ppf(0.975, n - 1)) * stdev(samples) / math.sqrt(n)
    return Estimate(n=n, mean=average, lower_95=average - half_width,
                    upper_95=average + half_width)


METRIC_FIELDS = (
    "instances",
    "missed_deadlines",
    "missed_deadline_pct",
    "mean_miss_delay_s",
    "rebinds",
    "completed_rebinds",
    "mean_redeploy_s",
    "overhead_messages",
    "overhead_per_rebind",
    "total_messages",
)


@dataclass(frozen=True)
class TableIRow:
    experiment: str
    orchestration: str
    metric: str
    n: int
    mean: float | None
    lower_95: float | None
    upper_95: float | None


def summarize_runs(runs: Sequence[RunMetrics]) -> list[TableIRow]:
    """Group by experiment+orchestration, preventing different scenarios being mixed."""
    groups: dict[tuple[str, str], list[RunMetrics]] = defaultdict(list)
    for run in runs:
        groups[run.experiment, run.orchestration].append(run)
    rows = []
    for (experiment, orchestration), group in sorted(groups.items()):
        seeds = [run.seed for run in group]
        if len(seeds) != len(set(seeds)):
            raise ValueError(f"duplicate seeds in {experiment}/{orchestration}")
        for metric in METRIC_FIELDS:
            summary = estimate_95(getattr(run, metric) for run in group)
            rows.append(TableIRow(experiment, orchestration, metric, **asdict(summary)))
    return rows


def build_table1(
    paths: Iterable[str | Path], measured_tasks: Iterable[str] | None = None
) -> list[TableIRow]:
    """Read event logs, compute run metrics, and aggregate by scenario/seed."""
    task_ids = tuple(measured_tasks) if measured_tasks is not None else None
    files = [Path(p) for p in paths]
    if not files:
        raise ValueError("at least one events.jsonl path is required")
    return summarize_runs([compute_from_file(p, task_ids) for p in files])


def write_table1_csv(path: str | Path, rows: Iterable[TableIRow]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(TableIRow.__dataclass_fields__))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Event logs -> Table I with 95% CIs")
    parser.add_argument("paths", nargs="+", type=Path, help="events.jsonl files or folders")
    parser.add_argument("-o", "--output", type=Path, default=Path("table1.csv"))
    parser.add_argument("--tasks", nargs="*", help="Optional task IDs to measure")
    args = parser.parse_args(argv)
    logs: list[Path] = []
    for path in args.paths:
        if path.is_dir():
            logs.extend(sorted(path.rglob("events.jsonl")))
        elif path.is_file():
            logs.append(path)
        else:
            parser.error(f"not found: {path}")
    rows = build_table1(logs, measured_tasks=args.tasks)
    write_table1_csv(args.output, rows)
    print(f"Wrote {len(rows)} Table I rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
