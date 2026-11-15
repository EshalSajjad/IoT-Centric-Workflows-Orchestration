"""Track 2's tests live inside its permitted folder (AGENTS.md boundary)."""

from __future__ import annotations

import pytest

from orchflow.core.events import DeviceLeft, MessageSent, RunEnded, RunStarted
from orchflow.core.testing import tiny_experiment, tiny_registry, tiny_workflow
from orchflow.metrics.compute import compute_metrics
from orchflow.runtime.sim.env import run_simulation
from orchflow.runtime.sim.mobility import ExponentialMobility


def test_echo_end_to_end_with_virtual_clock(tmp_path):
    experiment = tiny_experiment(duration_s=60)
    result = run_simulation(experiment, tiny_workflow(), tiny_registry(),
                            seed=5, event_log_path=tmp_path / "events.jsonl",
                            enable_mobility=False)
    assert result.elapsed_s == 60
    assert isinstance(result.events[0], RunStarted)
    assert isinstance(result.events[-1], RunEnded)
    # Six DEVICE_JOINs at time zero + six ticks * 2 peers * four messages.
    sent = [e for e in result.events if isinstance(e, MessageSent)]
    assert len(sent) == 6 + 6 * 2 * 4
    assert compute_metrics(result.events).instances == 6
    assert result.event_log_path and result.event_log_path.exists()


def test_same_seed_gives_identical_event_logs():
    experiment = tiny_experiment(duration_s=90)
    a = run_simulation(experiment, tiny_workflow(), tiny_registry(), seed=17)
    b = run_simulation(experiment, tiny_workflow(), tiny_registry(), seed=17)
    assert a.events == b.events
    assert any(isinstance(e, DeviceLeft) for e in a.events)


def test_departed_devices_stop_receiving():
    result = run_simulation(tiny_experiment(duration_s=3600), tiny_workflow(),
                            tiny_registry(n_peers=1, devices_per_peer=2), seed=42)
    assert len([e for e in result.events if isinstance(e, DeviceLeft)]) == 2


def test_exponential_distribution_is_seeded_and_has_five_minute_mean():
    import numpy as np

    model = ExponentialMobility()
    rng = np.random.default_rng(3)
    device = tiny_registry().device("dev-0001")
    samples = [model.residence_time_s(device, rng) for _ in range(100_000)]
    assert 290 < sum(samples) / len(samples) < 310
    with pytest.raises(ValueError):
        ExponentialMobility(mean_residence_s=0)
