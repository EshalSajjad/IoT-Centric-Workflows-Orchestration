"""``binding: {name: qos, params: {...}}`` -- choose by self-reported QoS.

NOT legacy behaviour: the legacy code stores Execution_Time / Cost / Response_Time in the
``services`` table but never reads them. This policy is the extension point later work
(e.g. trust-weighted binding) builds on. Lower score wins:

    score = w_exec * service.execution_time_s + w_response * device.specs.response_time_s
          + w_cost * service.cost             + w_distance * distance_km

Ties are broken by distance, then device id.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from orchflow.core.interfaces import Registry
from orchflow.core.models import Device, Location
from orchflow.core.plugins import register
from orchflow.core.spec import ServiceRequirement


@register("binding", "qos")
class QosBinding:
    def __init__(self, w_exec: float = 1.0, w_response: float = 1.0, w_cost: float = 0.0,
                 w_distance: float = 0.0) -> None:
        self.w_exec, self.w_response = w_exec, w_response
        self.w_cost, self.w_distance = w_cost, w_distance

    def score(self, req: ServiceRequirement, device: Device, near: Location,
              registry: Registry) -> float:
        svc = next((s for s in registry.services_of(device.id) if s.name == req.service_name),
                   None)
        exec_s = svc.execution_time_s if svc else 0.0
        cost = svc.cost if svc else 0.0
        return (self.w_exec * exec_s + self.w_response * device.specs.response_time_s
                + self.w_cost * cost + self.w_distance * device.location.distance_m(near) / 1000)

    def select(
        self,
        req: ServiceRequirement,
        candidates: Sequence[Device],
        near: Location,
        registry: Registry,
        rng: np.random.Generator,
    ) -> Device | None:
        if not candidates:
            return None
        return min(candidates, key=lambda d: (self.score(req, d, near, registry),
                                              d.location.distance_m(near), d.id))
