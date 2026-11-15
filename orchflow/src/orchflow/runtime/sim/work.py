"""Virtual deploy/execute durations, in seconds."""

from __future__ import annotations

import numpy as np

from orchflow.core.interfaces import WorkKind
from orchflow.core.models import Artifact, Service
from orchflow.core.plugins import register


@register("work", "fixed")
class FixedWork:
    """Service execution uses service.execution_time_s if available.

    Deploy includes constant setup time and optional bytes transferred, making
    large artifacts take longer without copying or executing any real files.
    """

    def __init__(
        self,
        execute_duration_s: float | None = None,
        deploy_duration_s: float = 0.5,
        transfer_bytes_per_s: float = 10_000_000.0,
    ) -> None:
        if execute_duration_s is not None and execute_duration_s < 0:
            raise ValueError("execute_duration_s must be nonnegative")
        if deploy_duration_s < 0 or transfer_bytes_per_s <= 0:
            raise ValueError("deploy_duration_s must be nonnegative and transfer rate positive")
        self.execute_duration_s = execute_duration_s
        self.deploy_duration_s = deploy_duration_s
        self.transfer_bytes_per_s = transfer_bytes_per_s

    def duration_s(
        self,
        kind: WorkKind,
        service: Service | None,
        artifact: Artifact | None,
        rng: np.random.Generator,
    ) -> float:
        if kind == "deploy":
            return self.deploy_duration_s + (
                artifact.size_bytes / self.transfer_bytes_per_s if artifact else 0.0
            )
        if kind == "execute":
            if self.execute_duration_s is not None:
                return self.execute_duration_s
            return service.execution_time_s if service is not None else 0.1
        raise ValueError(f"unknown work kind: {kind}")


@register("work", "sampled")
class SampledWork(FixedWork):
    """Fixed baseline with reproducible multiplicative lognormal jitter."""

    def __init__(
        self,
        variability: float = 0.1,
        execute_duration_s: float | None = None,
        deploy_duration_s: float = 0.5,
        transfer_bytes_per_s: float = 10_000_000.0,
    ) -> None:
        super().__init__(
            execute_duration_s=execute_duration_s,
            deploy_duration_s=deploy_duration_s,
            transfer_bytes_per_s=transfer_bytes_per_s,
        )
        if variability < 0:
            raise ValueError("variability must be nonnegative")
        self.variability = variability

    def duration_s(
        self,
        kind: WorkKind,
        service: Service | None,
        artifact: Artifact | None,
        rng: np.random.Generator,
    ) -> float:
        baseline = super().duration_s(kind, service, artifact, rng)
        return float(baseline * rng.lognormal(-self.variability**2 / 2, self.variability))
