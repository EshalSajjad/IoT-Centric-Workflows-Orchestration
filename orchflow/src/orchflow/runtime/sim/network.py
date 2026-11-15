"""Virtual-network latency models, in seconds."""

from __future__ import annotations

import numpy as np

from orchflow.core.messages import Message
from orchflow.core.plugins import register


@register("network", "ideal")
class IdealNetwork:
    def latency_s(self, msg: Message, rng: np.random.Generator) -> float:
        return 0.0


@register("network", "lan")
class LanNetwork:
    def __init__(self, base_latency_s: float = 0.002, jitter_s: float = 0.0) -> None:
        if base_latency_s < 0 or jitter_s < 0:
            raise ValueError("network latency and jitter must be nonnegative")
        self.base_latency_s = base_latency_s
        self.jitter_s = jitter_s

    def latency_s(self, msg: Message, rng: np.random.Generator) -> float:
        return max(0.0, float(self.base_latency_s + rng.normal(0, self.jitter_s)))


@register("network", "edge_wifi")
class EdgeWifiNetwork(LanNetwork):
    def __init__(self, base_latency_s: float = 0.03, jitter_s: float = 0.01) -> None:
        super().__init__(base_latency_s, jitter_s)
