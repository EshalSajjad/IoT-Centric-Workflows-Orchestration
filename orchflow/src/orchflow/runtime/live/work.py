"""Doing "work" for real: deploying an artifact or executing a service.

PLACEHOLDER. The legacy system SFTP'd a jar to the device and ran it. For now the live
runtime only *waits* as long as the work would take, so everything else (timers, messages,
on_work_done) is exercised for real. Swap SleepWork for a runner that really copies files /
starts processes when we get to the Oracle VM deployment.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from orchflow.core.interfaces import WorkKind
from orchflow.core.models import Artifact, Service


class WorkRunner(Protocol):
    async def run(self, kind: WorkKind, service: Service | None, artifact: Artifact | None,
                  data: dict[str, Any]) -> bool:
        """Do the work; return True on success."""
        ...


class SleepWork:
    """execute = wait service.execution_time_s; deploy = wait size / bandwidth."""

    def __init__(self, deploy_bandwidth_bytes_s: float = 1_000_000.0) -> None:
        self.bandwidth = deploy_bandwidth_bytes_s

    async def run(self, kind: WorkKind, service: Service | None, artifact: Artifact | None,
                  data: dict[str, Any]) -> bool:
        if kind == "execute":
            await asyncio.sleep(service.execution_time_s if service else 0.0)
        else:
            await asyncio.sleep(artifact.size_bytes / self.bandwidth if artifact else 0.0)
        return True
