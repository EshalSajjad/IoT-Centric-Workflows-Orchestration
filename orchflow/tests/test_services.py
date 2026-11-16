"""Track 1: service stubs for the live runtime."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from orchflow.core.spec import WorkflowSpec, load_yaml
from orchflow.services import STUBS, run_service

ROOT = Path(__file__).resolve().parents[1]


def test_every_imw_device_service_has_a_stub() -> None:
    wf = load_yaml(ROOT / "workflows" / "imw.yaml", WorkflowSpec)
    names = {r.service_name for t in wf.tasks for r in t.requirements if r.kind.value != "cloud"}
    assert names <= set(STUBS), sorted(names - set(STUBS))


def test_sensor_readings_use_legacy_units_and_are_seeded() -> None:
    a = run_service("temperature", np.random.default_rng(1))
    b = run_service("temperature", np.random.default_rng(1))
    assert a == b and a["unit"] == "°C" and 20 <= a["value"] <= 30
    assert run_service("wind_speed", np.random.default_rng(2))["reading"].endswith(" knots")


def test_unknown_and_cloud_services_still_run() -> None:
    assert run_service("not_a_service", np.random.default_rng(0)) == {"service": "not_a_service"}
    assert run_service(None, np.random.default_rng(0)) == {"service": None}
