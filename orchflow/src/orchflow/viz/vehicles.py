"""GPS vehicle simulator: a port of legacy's ``gps-vehicle-simulator`` jar, for the map.

Legacy (``workflow-coordinator/gps-vehicle-simulator-1.0.0.BUILD-SNAPSHOT.jar``, Spring Boot):
the vehicle_status_service task starts it; ``/api/dc`` loads the trips in
``directions.json``, routes them, and writes ``map_display.html``, a Google Map with each
route as a red polyline and a car marker moving along it at 40 km/h. ``/api/status`` returns
each car's current position (the vehicle_status_service output, which the coordinator parses
to find each car's closest peer node, ``Workflow_Coordinator_Main.parse_vehicle_status``).

Here the cars move on the same trips (routes precomputed by ``viz/routes.py``) at the same
40 km/h, from the moment the vehicle_status_service task first starts. Positions are a pure
function of time, so the dashboard just asks for them when it draws.

Re-routing: legacy's ``re_route_vehicle_service`` was a SOAP call to the authors' server,
which is not in the repository (docs/deviations.md D17). Our stand-in only checks each car's
remaining route against the plume's threat zone and stops a car that would drive into it.
With legacy's ALOHA plume none of the three trips crosses the threat zone, so nothing
changes, which matches the demo video.
"""

from __future__ import annotations

import math
from typing import Any

from orchflow.core.models import Location
from orchflow.viz.routes import load_routes

SPEED_KPH = 40.0  # legacy: setSpeedInKph / "var speed = 40; // km/h" in map_display.html

Ring = list[tuple[float, float]]  # (lon, lat)


def _length(path: list[list[float]]) -> list[float]:
    """Cumulative distance (m) at each point of a [[lat, lon], ...] path."""
    out = [0.0]
    for (a1, b1), (a2, b2) in zip(path, path[1:], strict=False):
        out.append(out[-1] + Location(lat=a1, lon=b1).distance_m(Location(lat=a2, lon=b2)))
    return out


def _inside(ring: Ring, lon: float, lat: float) -> bool:
    c = False
    for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1], strict=True):
        if (y1 > lat) != (y2 > lat) and lon < x1 + (lat - y1) * (x2 - x1) / (y2 - y1):
            c = not c
    return c


def crosses(path: list[list[float]], ring: Ring, step_m: float = 10.0) -> bool:
    """Does the [[lat, lon], ...] path enter the (lon, lat) polygon? (sampled every ~10 m)"""
    for (a1, b1), (a2, b2) in zip(path, path[1:], strict=False):
        seg = Location(lat=a1, lon=b1).distance_m(Location(lat=a2, lon=b2))
        n = max(1, math.ceil(seg / step_m))
        if any(_inside(ring, b1 + (b2 - b1) * k / n, a1 + (a2 - a1) * k / n)
               for k in range(n + 1)):
            return True
    return False


class Vehicle:
    def __init__(self, vid: str, trip: dict[str, Any]) -> None:
        self.id, self.origin, self.destination = vid, trip["from"], trip["to"]
        self.status = "driving"
        self.path: list[list[float]] = trip["path"]
        self.cum = _length(self.path)
        self.offset_m = 0.0  # where a stopped car stands

    def travelled_m(self, elapsed_s: float) -> float:
        if self.status == "blocked":
            return self.offset_m
        return min(self.cum[-1], self.offset_m + elapsed_s * SPEED_KPH / 3.6)

    def position(self, elapsed_s: float) -> list[float]:
        d = self.travelled_m(elapsed_s)
        for i in range(1, len(self.cum)):
            if self.cum[i] >= d:
                seg = self.cum[i] - self.cum[i - 1] or 1.0
                f = (d - self.cum[i - 1]) / seg
                (a1, b1), (a2, b2) = self.path[i - 1], self.path[i]
                return [a1 + (a2 - a1) * f, b1 + (b2 - b1) * f]
        return list(self.path[-1])

    def remaining(self, elapsed_s: float) -> list[list[float]]:
        d = self.travelled_m(elapsed_s)
        i = next((k for k, c in enumerate(self.cum) if c >= d), len(self.path) - 1)
        return [self.position(elapsed_s)] + self.path[i:]


class VehicleSimulator:
    """The cars of the demo. ``start(t)`` when vehicle_status_service first runs."""

    def __init__(self, trips: list[dict[str, Any]] | None = None) -> None:
        trips = load_routes() if trips is None else trips
        self.vehicles = [Vehicle(f"vehicle-{i + 1}", t) for i, t in enumerate(trips)]
        self.started_at: float | None = None

    def start(self, t: float) -> None:
        if self.started_at is None:
            self.started_at = t

    def elapsed(self, t: float) -> float:
        return 0.0 if self.started_at is None else max(0.0, t - self.started_at)

    def reroute(self, t: float, threat: Ring | None) -> list[str]:
        """Stop cars whose remaining route enters the threat zone; returns log lines."""
        if threat is None or self.started_at is None:
            return []
        notes, el = [], self.elapsed(t)
        for v in self.vehicles:
            if v.status != "driving" or not crosses(v.remaining(el), threat):
                continue
            v.offset_m, v.status = v.travelled_m(el), "blocked"
            notes.append(f"{v.id}: route enters the threat zone: stopped")
        return notes

    def snapshot(self, t: float) -> list[dict[str, Any]]:
        el = self.elapsed(t)
        out = []
        for v in self.vehicles:
            done = v.travelled_m(el) >= v.cum[-1]
            out.append({"id": v.id, "from": v.origin, "to": v.destination,
                        "path": v.path, "position": v.position(el) if self.started_at
                        is not None else None,
                        "status": "arrived" if done else v.status,
                        "speed_kph": SPEED_KPH})
        return out
