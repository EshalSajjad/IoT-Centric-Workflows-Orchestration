"""Plume computation: sensor readings -> toxic threat zones as GeoJSON.

The legacy demo ran NOAA ALOHA on the device and shipped its KML (threat zones for a benzene
release: red 4000 ppm / orange 800 ppm / yellow 52 ppm, the AEGL-3/2/1 60-min levels). ALOHA
is a desktop program we can't run, so we compute the same kind of picture with the textbook
model ALOHA uses for neutrally buoyant gases: a ground-level **Gaussian plume**.

    C(x, y) = Q / (pi * u * sy(x) * sz(x)) * exp(-y^2 / (2 sy(x)^2))      [g/m^3]

x = metres downwind, y = metres crosswind, u = wind speed (m/s), Q = release rate (g/s),
sy / sz = Briggs rural dispersion coefficients for the Pasquill stability class (picked from
the wind speed, daytime). A zone's edge is where C equals its threshold.

Inputs are the instance's sensor readings, as the services return them (``services/__init__``):
wind_speed (knots), wind_direction (degrees the wind blows FROM), temperature (deg C, for the
ppm -> mg/m^3 conversion) and aerosol_concentration (scales the release, a stand-in for the
source strength). Not a validated model: it gives the right shape, direction and rough size.
"""

from __future__ import annotations

import math
from typing import Any

from orchflow.core.models import Location
from orchflow.core.registry import offset

KNOT_M_S = 0.514444
BENZENE_MW = 78.11  # g/mol
# AEGL 60-min levels for benzene, as in the demo's ALOHA output
ZONES = (("red", 4000.0, "#d32f2f"), ("orange", 800.0, "#f57c00"), ("yellow", 52.0, "#fbc02d"))
DEFAULT_RELEASE_G_S = 3300.0  # gives ALOHA-like extents (~500 m yellow at 7 mph, class C)

# Briggs (1973) rural sigma_y, sigma_z (x in metres)
_SIGMA = {
    "A": (lambda x: 0.22 * x / math.sqrt(1 + 1e-4 * x), lambda x: 0.20 * x),
    "B": (lambda x: 0.16 * x / math.sqrt(1 + 1e-4 * x), lambda x: 0.12 * x),
    "C": (lambda x: 0.11 * x / math.sqrt(1 + 1e-4 * x),
          lambda x: 0.08 * x / math.sqrt(1 + 2e-4 * x)),
    "D": (lambda x: 0.08 * x / math.sqrt(1 + 1e-4 * x),
          lambda x: 0.06 * x / math.sqrt(1 + 1.5e-3 * x)),
}


def stability_class(wind_m_s: float) -> str:
    """Pasquill class for daytime, moderate sun (simplified)."""
    if wind_m_s < 2:
        return "A"
    if wind_m_s < 3:
        return "B"
    if wind_m_s < 5:
        return "C"
    return "D"


def ppm_to_g_m3(ppm: float, temperature_c: float, mw: float = BENZENE_MW) -> float:
    molar_volume_l = 22.414 * (temperature_c + 273.15) / 273.15
    return ppm * mw / molar_volume_l / 1000.0


def _value(readings: dict[str, Any], name: str, default: float) -> float:
    r = readings.get(name)
    if isinstance(r, dict) and isinstance(r.get("value"), int | float):
        return float(r["value"])
    return default


def compute_plume(readings: dict[str, Any], source: Location, *,
                  release_g_s: float = DEFAULT_RELEASE_G_S, points: int = 48) -> dict[str, Any]:
    """GeoJSON FeatureCollection: one polygon per threat zone (largest first) + the source."""
    wind_m_s = max(0.5, _value(readings, "wind_speed", 6.0) * KNOT_M_S)
    from_deg = _value(readings, "wind_direction", 225.0) % 360
    temp_c = _value(readings, "temperature", 25.0)
    q = release_g_s * min(2.0, max(0.25, _value(readings, "aerosol_concentration", 50.0) / 50))
    cls = stability_class(wind_m_s)
    sy, sz = _SIGMA[cls]

    to_rad = math.radians((from_deg + 180) % 360)  # the plume travels downwind
    down = (math.sin(to_rad), math.cos(to_rad))  # (east, north)
    cross = (math.cos(to_rad), -math.sin(to_rad))

    def centre(x: float) -> float:
        return q / (math.pi * wind_m_s * sy(x) * sz(x))

    def at(x: float, y: float) -> list[float]:
        loc = offset(source, x * down[0] + y * cross[0], x * down[1] + y * cross[1])
        return [round(loc.lon, 7), round(loc.lat, 7)]

    features: list[dict[str, Any]] = []
    for name, ppm, colour in reversed(ZONES):  # yellow first, so smaller zones draw on top
        thr = ppm_to_g_m3(ppm, temp_c)
        # downwind extent: centreline concentration falls below the threshold (monotone)
        lo, hi = 1.0, 1.0
        while centre(hi) > thr and hi < 50_000:
            hi *= 2
        if centre(lo) <= thr:
            continue  # never reaches this level
        for _ in range(60):
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if centre(mid) > thr else (lo, mid)
        extent = lo
        xs = [extent * (i / points) ** 1.5 for i in range(1, points + 1)]
        half = [sy(x) * math.sqrt(2 * math.log(centre(x) / thr)) if centre(x) > thr else 0.0
                for x in xs]
        ring = ([at(0, 0)] + [at(x, w) for x, w in zip(xs, half, strict=True)]
                + [at(x, -w) for x, w in reversed(list(zip(xs, half, strict=True)))]
                + [at(0, 0)])
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {"zone": name, "threshold_ppm": ppm, "color": colour,
                           "extent_m": round(extent, 1)},
        })
    features.append({
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [source.lon, source.lat]},
        "properties": {"zone": "source"},
    })
    return {
        "type": "FeatureCollection",
        "features": features,
        "properties": {"wind_m_s": round(wind_m_s, 2), "wind_from_deg": round(from_deg, 1),
                       "temperature_c": temp_c, "stability": cls, "release_g_s": round(q, 1),
                       "chemical": "benzene"},
    }
