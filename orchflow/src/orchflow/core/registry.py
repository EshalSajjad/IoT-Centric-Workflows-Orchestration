"""The registry: every peer, cell, device and service in the testbed (Track 1).

Replaces the legacy MySQL ``devices`` / ``services`` / ``peer_nodes`` tables. Instead of
shipping a database dump with hardcoded Raspberry Pi IPs, the registry is GENERATED from a
``TestbedSpec`` and its ``registry_seed``, so every node (and every run) that builds it from
the same YAML gets an identical copy.

    registry = build_registry(load_yaml("configs/testbed_20x20.yaml", TestbedSpec),
                              workflow=load_yaml("workflows/plume.yaml", WorkflowSpec))

Layout
------
* ``n_peers`` cells on a square grid centred on ``area_center``; neighbouring cells touch
  (centre spacing = 2 x ``cell_radius_m``). One peer node per cell, at the cell centre.
* ``devices_per_peer`` devices per cell, placed uniformly at random inside the cell.
* Every device offers every service in ``services_per_device``. This mirrors the legacy
  ``gom.sql``, where each device row offers the full list of IMW services.
* A service's kind / deployable flag / artifact come from the workflow's requirements when a
  workflow is given; otherwise a service defaults to a non-deployable sensor.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence

import numpy as np

from orchflow.core.models import (
    Artifact,
    Cell,
    Device,
    DeviceSpecs,
    Location,
    PeerNode,
    Service,
    ServiceKind,
)
from orchflow.core.spec import ServiceRequirement, TestbedSpec, WorkflowSpec

METRES_PER_DEG_LAT = 111_320.0

# Defaults for generated services. Execution times are what the sim's "fixed" work model can
# read from Service.execution_time_s; they are not from the legacy DB (whose values were
# placeholders), see docs/deviations.md.
DEFAULT_EXEC_S = {ServiceKind.SENSOR: 0.2, ServiceKind.FOG: 2.0, ServiceKind.CLOUD: 1.0}
DEFAULT_ARTIFACT_BYTES = 2_000_000


class InMemoryRegistry:
    """The Registry implementation used by both runtimes. Availability is the only mutable
    state: node logic marks a device unavailable when it detects the departure and available
    again when the device re-joins."""

    def __init__(
        self,
        devices: Iterable[Device],
        peers: Iterable[PeerNode],
        cells: Iterable[Cell],
        services: Iterable[Service],
    ) -> None:
        self._devices = {d.id: d for d in devices}
        self._peers = {p.id: p for p in peers}
        self._cells = list(cells)
        self._services: dict[str, list[Service]] = {}
        for s in services:
            if s.device_id not in self._devices:
                raise ValueError(f"service {s.id} is on unknown device {s.device_id}")
            self._services.setdefault(s.device_id, []).append(s)
        self._available = dict.fromkeys(self._devices, True)

    # --- lookups
    def devices(self) -> Sequence[Device]:
        return list(self._devices.values())

    def device(self, device_id: str) -> Device:
        return self._devices[device_id]

    def peers(self) -> Sequence[PeerNode]:
        return list(self._peers.values())

    def peer(self, peer_id: str) -> PeerNode:
        return self._peers[peer_id]

    def cells(self) -> Sequence[Cell]:
        return list(self._cells)

    def services_of(self, device_id: str) -> Sequence[Service]:
        return list(self._services.get(device_id, ()))

    def candidates(
        self,
        req: ServiceRequirement,
        near: Location,
        *,
        cell_id: str | None = None,
        exclude: frozenset[str] = frozenset(),
    ) -> Sequence[Device]:
        out = []
        for d in self._devices.values():  # insertion order = id order (deterministic)
            if d.id in exclude or not self._available[d.id]:
                continue
            if cell_id is not None and d.cell_id != cell_id:
                continue
            if not any(s.name == req.service_name for s in self._services.get(d.id, ())):
                continue
            if d.specs.ram_gb < req.min_ram_gb or d.specs.storage_gb < req.min_storage_gb:
                continue
            if req.radius_m is not None and d.location.distance_m(near) > req.radius_m:
                continue
            out.append(d)
        return out

    # --- availability
    def set_available(self, device_id: str, available: bool) -> None:
        if device_id not in self._available:
            raise KeyError(device_id)
        self._available[device_id] = available

    def is_available(self, device_id: str) -> bool:
        return self._available[device_id]


# --------------------------------------------------------------------------- generation


def offset(origin: Location, east_m: float, north_m: float) -> Location:
    """Location ``east_m`` metres east and ``north_m`` metres north of ``origin``
    (flat-earth approximation; fine at testbed scale)."""
    dlat = north_m / METRES_PER_DEG_LAT
    dlon = east_m / (METRES_PER_DEG_LAT * math.cos(math.radians(origin.lat)))
    return Location(lat=origin.lat + dlat, lon=origin.lon + dlon)


def service_catalog(workflow: WorkflowSpec | None) -> dict[str, ServiceRequirement]:
    """service name -> the first requirement that names it (gives kind / deployable / artifact)."""
    out: dict[str, ServiceRequirement] = {}
    if workflow is not None:
        for task in workflow.tasks:
            for req in task.requirements:
                out.setdefault(req.service_name, req)
    return out


def build_registry(testbed: TestbedSpec, workflow: WorkflowSpec | None = None) -> InMemoryRegistry:
    rng = np.random.default_rng(testbed.registry_seed)
    catalog = service_catalog(workflow)
    names = list(dict.fromkeys(testbed.services_per_device))  # de-duplicate, keep order

    cols = math.ceil(math.sqrt(testbed.n_peers))
    rows = math.ceil(testbed.n_peers / cols)
    spacing = 2 * testbed.cell_radius_m

    cells: list[Cell] = []
    peers: list[PeerNode] = []
    devices: list[Device] = []
    services: list[Service] = []
    k = 0
    for p in range(testbed.n_peers):
        r, c = divmod(p, cols)
        center = offset(
            testbed.area_center,
            (c - (cols - 1) / 2) * spacing,
            ((rows - 1) / 2 - r) * spacing,
        )
        cell_id, peer_id = f"cell-{p + 1:02d}", f"peer-{p + 1:02d}"
        cells.append(Cell(id=cell_id, center=center, radius_m=testbed.cell_radius_m))
        peers.append(PeerNode(id=peer_id, name=f"peer node {p + 1}", cell_id=cell_id,
                              location=center))
        for _ in range(testbed.devices_per_peer):
            k += 1
            dev_id = f"dev-{k:04d}"
            # uniform in the disc: sqrt for the radius
            rad = testbed.cell_radius_m * 0.95 * math.sqrt(rng.random())
            ang = 2 * math.pi * rng.random()
            devices.append(Device(
                id=dev_id, name=f"device {k}", peer_id=peer_id, cell_id=cell_id,
                location=offset(center, rad * math.cos(ang), rad * math.sin(ang)),
                specs=DeviceSpecs(),
            ))
            for name in names:
                services.append(_make_service(dev_id, name, catalog.get(name)))
    return InMemoryRegistry(devices, peers, cells, services)


def _make_service(device_id: str, name: str, req: ServiceRequirement | None) -> Service:
    kind = req.kind if req else ServiceKind.SENSOR
    deployable = req.deployable if req else False
    artifact = None
    if deployable:
        artifact = Artifact(name=(req.artifact if req and req.artifact else f"{name}.jar"),
                            size_bytes=DEFAULT_ARTIFACT_BYTES)
    return Service(
        id=f"{device_id}/{name}", name=name, kind=kind, deployable=deployable,
        device_id=device_id, execution_time_s=DEFAULT_EXEC_S[kind], artifact=artifact,
    )
