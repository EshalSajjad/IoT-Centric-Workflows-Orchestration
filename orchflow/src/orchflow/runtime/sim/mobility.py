"""Seeded mobility models shared by the simulator and the live runtime.

Residence times use seconds. Pass a node-specific numpy Generator supplied by the
runtime so two runs with the same seed have identical mobility events.
"""

from __future__ import annotations

import numpy as np

from orchflow.core.models import Device
from orchflow.core.plugins import register


@register("mobility", "exponential")
class ExponentialMobility:
    """Exponentially distributed departures (default mean: five minutes).

    ``mean_rejoin_s=None`` means a departed device never returns. When a rejoin
    time is provided, residence and rejoin delays are independently sampled.
    """

    def __init__(
        self,
        mean_residence_s: float = 300.0,
        mean_rejoin_s: float | None = None,
        graceful_probability: float = 1.0,
    ) -> None:
        if mean_residence_s <= 0:
            raise ValueError("mean_residence_s must be positive")
        if mean_rejoin_s is not None and mean_rejoin_s <= 0:
            raise ValueError("mean_rejoin_s must be positive or None")
        if not 0 <= graceful_probability <= 1:
            raise ValueError("graceful_probability must be between 0 and 1")
        self.mean_residence_s = mean_residence_s
        self.mean_rejoin_s = mean_rejoin_s
        self.graceful_probability = graceful_probability

    def residence_time_s(self, device: Device, rng: np.random.Generator) -> float:
        return float(rng.exponential(self.mean_residence_s))

    def rejoin_delay_s(self, device: Device, rng: np.random.Generator) -> float | None:
        if self.mean_rejoin_s is None:
            return None
        return float(rng.exponential(self.mean_rejoin_s))

    def is_graceful(self, device: Device, rng: np.random.Generator) -> bool:
        if self.graceful_probability == 1:
            return True
        if self.graceful_probability == 0:
            return False
        return bool(rng.random() < self.graceful_probability)
