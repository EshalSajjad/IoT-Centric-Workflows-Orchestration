"""Registry data: the things that exist in the testbed (devices, peers, cells, services).

SHARED CONTRACT. Every track imports these. Change only via a contract PR
(see docs/contracts.md, "Changing the contract").

Conventions
-----------
* IDs are strings. Node IDs double as transport addresses:
    coordinator  -> "coord"
    peer node    -> "peer-01", "peer-02", ...
    end device   -> "dev-0001", "dev-0002", ...
* Units are SI and spelled out in field names: seconds (``_s``), metres (``_m``),
  gigabytes (``_gb``). No minutes, no milliseconds, no unit-less numbers.
* All models are immutable (frozen). Change state in the Registry, not on the object.
"""

from __future__ import annotations

import math
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

COORDINATOR_ID = "coord"


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Location(Frozen):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)

    def distance_m(self, other: Location) -> float:
        """Great-circle (haversine) distance in metres."""
        r = 6_371_000.0
        p1, p2 = math.radians(self.lat), math.radians(other.lat)
        dp = p2 - p1
        dl = math.radians(other.lon - self.lon)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))


class ServiceKind(StrEnum):
    SENSOR = "sensor"  # non-deployable data source (wind_speed, camera_feed, ...)
    FOG = "fog"  # runs on an end device / peer near the data (plume_service, ...)
    CLOUD = "cloud"  # runs off-site (impose_plume_service, re_route_vehicle, ...)


class Cell(Frozen):
    """A geographic area served by exactly one peer node."""

    id: str
    center: Location
    radius_m: float = Field(gt=0)


class PeerNode(Frozen):
    id: str  # "peer-01"
    name: str
    cell_id: str
    location: Location
    address: str | None = None  # live runtime only (host or Tailscale IP); None in sim


class DeviceSpecs(Frozen):
    """Self-reported hardware/QoS figures. Unverified, exactly as in the paper."""

    cpu_ghz: float = 1.2
    ram_gb: float = 1.0
    storage_gb: float = 8.0
    battery_pct: float = Field(default=100.0, ge=0, le=100)
    response_time_s: float = Field(default=0.1, ge=0)


class Device(Frozen):
    id: str  # "dev-0001"
    name: str
    peer_id: str  # peer node responsible for this device's cell
    cell_id: str
    location: Location
    specs: DeviceSpecs = DeviceSpecs()
    address: str | None = None  # live runtime only


class Artifact(Frozen):
    """Something that must be transferred to a device before a deployable service can run
    (the legacy system SFTP'd a jar + config files)."""

    name: str
    size_bytes: int = Field(ge=0)


class Service(Frozen):
    """One service instance offered by one device."""

    id: str
    name: str  # matches ServiceRequirement.service_name, e.g. "wind_speed"
    kind: ServiceKind
    deployable: bool  # True -> needs an Artifact deployed before it can run
    device_id: str
    execution_time_s: float = Field(default=0.0, ge=0)
    response_time_s: float = Field(default=0.0, ge=0)
    cost: float = Field(default=0.0, ge=0)
    artifact: Artifact | None = None
