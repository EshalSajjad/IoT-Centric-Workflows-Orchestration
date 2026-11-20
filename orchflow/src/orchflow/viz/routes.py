"""Vehicle routes for the GPS vehicle simulator (legacy ``gps-vehicle-simulator`` jar).

Legacy read ``directions.json`` (3 trips, as street addresses), asked the Google Directions
API for each route and animated a car along it at 40 km/h. We can't call Google without a
key, so the routes are fetched ONCE from OpenStreetMap services (Nominatim geocoding + the
public OSRM router) and stored in ``viz/data/vehicle_routes.json``; runs never need the
network. Regenerate with::

    python -m orchflow.viz.routes
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DATA = Path(__file__).with_name("data")
DIRECTIONS = DATA / "directions.json"  # copied verbatim from the simulator jar
ROUTES = DATA / "vehicle_routes.json"
_UA = {"User-Agent": "orchflow-sproj-route-builder/0.1"}


def load_routes(path: Path = ROUTES) -> list[dict[str, Any]]:
    """[{from, to, path: [[lat, lon], ...]}, ...]"""
    return json.loads(path.read_text(encoding="utf-8"))["trips"]


def _get(url: str) -> Any:
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _geocode(address: str) -> tuple[float, float]:
    q = urllib.parse.urlencode({"q": address, "format": "json", "limit": 1})
    hits = _get(f"https://nominatim.openstreetmap.org/search?{q}")
    if not hits:  # retry without the business name ("Place, 12 Street, City" -> street part)
        q = urllib.parse.urlencode({"q": address.split(",", 1)[1], "format": "json", "limit": 1})
        hits = _get(f"https://nominatim.openstreetmap.org/search?{q}")
    time.sleep(1.1)  # Nominatim usage policy: at most 1 request per second
    return float(hits[0]["lat"]), float(hits[0]["lon"])


def _route(a: tuple[float, float], b: tuple[float, float]) -> list[list[float]]:
    url = (f"https://router.project-osrm.org/route/v1/driving/{a[1]},{a[0]};{b[1]},{b[0]}"
           "?overview=full&geometries=geojson")
    coords = _get(url)["routes"][0]["geometry"]["coordinates"]
    return [[round(lat, 6), round(lon, 6)] for lon, lat in coords]


def build() -> dict[str, Any]:
    trips = []
    for d in json.loads(DIRECTIONS.read_text(encoding="utf-8")):
        a, b = _geocode(d["from"]), _geocode(d["to"])
        trips.append({"from": d["from"], "to": d["to"], "path": _route(a, b)})
    return {"source": "Nominatim + OSRM (OpenStreetMap), built by python -m orchflow.viz.routes",
            "trips": trips}


def main() -> None:
    data = build()
    ROUTES.write_text(json.dumps(data), encoding="utf-8")
    for t in data["trips"]:
        print(f"{t['from'][:40]:40} -> {t['to'][:40]:40} {len(t['path'])} points")


if __name__ == "__main__":
    main()
