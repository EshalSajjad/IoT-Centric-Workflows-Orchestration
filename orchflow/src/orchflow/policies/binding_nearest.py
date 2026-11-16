"""``binding: {name: nearest}`` -- the paper's behaviour.

Port of legacy ``Helper.findMinimal`` / ``getMin`` (workflow-coordinator/src/util/Helper.java):
pick the candidate closest to the task location. Legacy kept the FIRST device on a tie
(strict ``<`` while scanning in DB id order); we break ties by device id, which is the same.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from orchflow.core.interfaces import Registry
from orchflow.core.models import Device, Location
from orchflow.core.plugins import register
from orchflow.core.spec import ServiceRequirement


@register("binding", "nearest")
class NearestBinding:
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
        return min(candidates, key=lambda d: (d.location.distance_m(near), d.id))
