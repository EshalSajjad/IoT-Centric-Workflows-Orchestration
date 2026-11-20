"""Read the legacy ALOHA threat-zone KML into GeoJSON for the map.

Legacy never ran ALOHA during a run: ``data_integration_service.jar`` was handed a KML made
beforehand with NOAA ALOHA (``end-device/output_0.kml``: benzene, 9 mph from the SW,
June 20 2016) and the coordinator showed it on a Google Map (``plume_display.html``, written
by the GPS simulator jar, loads it with ``google.maps.KmlLayer``). We ship that same file
(``viz/data/aloha_output_0.kml``) and draw it the same way.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

DATA = Path(__file__).with_name("data")
ALOHA_KML = DATA / "aloha_output_0.kml"
ALOHA_IMAGE = DATA / "aloha_0.jpg"
_NS = {"k": "http://www.opengis.net/kml/2.2"}

# colours of ALOHA's own legend (red / orange / yellow threat zones)
_ZONES = (("red", "#ff0000"), ("orange", "#ff9900"), ("yellow", "#ffff00"))


def _coords(text: str) -> list[list[float]]:
    out = []
    for tok in text.split():
        lon, lat, *_ = (float(v) for v in tok.split(","))
        out.append([lon, lat])
    return out


def read_aloha_kml(path: Path = ALOHA_KML) -> dict[str, Any]:
    """GeoJSON FeatureCollection with one feature per placemark, plus the ALOHA summary."""
    root = ET.parse(path).getroot()
    features: list[dict[str, Any]] = []
    summary = ""
    for pm in root.iter(f"{{{_NS['k']}}}Placemark"):
        name = (pm.findtext("k:name", default="", namespaces=_NS) or "").strip()
        desc = pm.findtext("k:description", default="", namespaces=_NS) or ""
        if desc and not summary:
            summary = desc
        zone = next((z for z, _ in _ZONES if name.lower().startswith(z)), "source")
        colour = dict(_ZONES).get(zone, "#000000")
        props = {"name": name, "zone": zone, "color": colour,
                 "confidence_lines": "confidence" in name.lower()}
        poly = pm.find(".//k:Polygon//k:coordinates", _NS)
        point = pm.find(".//k:Point/k:coordinates", _NS)
        if poly is not None and poly.text:
            geom = {"type": "Polygon", "coordinates": [_coords(poly.text)]}
        elif point is not None and point.text:
            geom = {"type": "Point", "coordinates": _coords(point.text)[0]}
        else:
            continue
        features.append({"type": "Feature", "geometry": geom, "properties": props})
    return {"type": "FeatureCollection", "features": features,
            "properties": _summary(summary)}


def _summary(html: str) -> dict[str, str]:
    """The fields ALOHA writes in the placemark description (time, chemical, wind, zones)."""
    text = re.sub(r"<br\s*/?>", "\n", html)
    text = re.sub(r"<[^>]+>", "", text).replace("&nbsp;", " ")
    out: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        key, value = key.strip(), " ".join(value.split())
        if sep and value and key in {"Time", "Chemical Name", "Wind", "Model",
                                     "Red", "Orange", "Yellow"}:
            out[key.lower().replace(" ", "_")] = value
    return out


def threat_polygon(plume: dict[str, Any]) -> list[tuple[float, float]] | None:
    """(lon, lat) ring of the outermost drawn threat zone (yellow), or None."""
    for f in plume["features"]:
        p = f["properties"]
        if (f["geometry"]["type"] == "Polygon" and p.get("zone") == "yellow"
                and not p.get("confidence_lines")):
            return [(x, y) for x, y in f["geometry"]["coordinates"][0]]
    return None
