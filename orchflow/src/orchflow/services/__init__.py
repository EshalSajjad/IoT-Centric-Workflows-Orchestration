"""TRACK 1 (logic): service stubs used by the live runtime.

When the live runtime gets an ``execute`` work request (``exec:`` on a device, ``cloud:`` on
the coordinator, see docs/protocol.md §5), it calls :func:`run_service` and returns the dict
as the work's ``data["output"]``. The simulator doesn't need these (it only samples durations).

Why stubs: the legacy sensors read a SOAP sensor server (``Main_Rasp_IoT.java:2316-2450``) and
the fog/cloud services were jars (``data_integration_service.jar`` …). Neither the server nor
the real jars are in the repo, so we produce readings of the same shape and units as legacy.
Readings are random but drawn from the ``rng`` the runtime passes in (``ctx.rng``), so a seeded
run is reproducible. Ranges marked "legacy" come from the code; the rest are plausible guesses
(docs/deviations.md D16).

Add a service with ``@stub("my_service")``. A service with no stub still runs: it returns
``{"service": name}``, so any imported workflow works live without new code.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import numpy as np

ServiceFn = Callable[[np.random.Generator, dict[str, Any]], dict[str, Any]]

STUBS: dict[str, ServiceFn] = {}


def stub(name: str) -> Callable[[ServiceFn], ServiceFn]:
    def deco(fn: ServiceFn) -> ServiceFn:
        if name in STUBS:
            raise ValueError(f"service stub {name!r} registered twice")
        STUBS[name] = fn
        return fn
    return deco


def run_service(name: str | None, rng: np.random.Generator,
                inputs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run one service once. ``name=None`` is the task's cloud part (``cloud:`` work)."""
    fn = STUBS.get(name or "")
    out = fn(rng, inputs or {}) if fn else {}
    return {"service": name, **out}


def _reading(name: str, lo: int, hi: int, unit: str) -> None:
    """Legacy sensors return one integer + unit, e.g. ``"12 knots"``."""
    @stub(name)
    def _read(rng: np.random.Generator, inputs: dict[str, Any]) -> dict[str, Any]:
        value = int(rng.integers(lo, hi + 1))
        return {"value": value, "unit": unit, "reading": f"{value} {unit}"}


# --- sensors (IMW tasks 1 and 4); units are legacy's -------------------------------------
_reading("wind_speed", 0, 40, "knots")            # unit: Main_Rasp_IoT.java:2337
_reading("wind_direction", 0, 359, "azimuth")     # unit: :2360
_reading("temperature", 20, 30, "°C")             # range + unit: :2156, :2383 (legacy)
_reading("humidity", 0, 100, "RH")                # unit: :2406
_reading("aerosol_concentration", 0, 100, "cm")   # unit: :2429 (sic)
_reading("camera_feed_intersection_1", 0, 60, "vehicles")
_reading("vehicle_status_service", 0, 60, "vehicles")


# --- fog / cloud services --------------------------------------------------------------
@stub("plume_service")
def _plume(rng: np.random.Generator, inputs: dict[str, Any]) -> dict[str, Any]:
    # legacy: data_integration_service.jar wrote output_0.kml from the sensor readings
    return {"artifact": "output_0.kml", "inputs": sorted(inputs)}


@stub("traffic_flow_service")
def _traffic(rng: np.random.Generator, inputs: dict[str, Any]) -> dict[str, Any]:
    # legacy: Traffic_Flow_Finder.jar; we report a flow level per intersection
    return {"flow": str(rng.choice(["free", "moderate", "congested"])), "inputs": sorted(inputs)}


@stub("computation_service_plume")
def _plume_compute(rng: np.random.Generator, inputs: dict[str, Any]) -> dict[str, Any]:
    # non-deployable compute step of IMW task 1; no legacy code, placeholder result
    return {"result": "plume_computed", "inputs": sorted(inputs)}
