"""Input files: what to run (workflow), on what (testbed), and how (experiment).

SHARED CONTRACT. All three are YAML files validated by these models.

    workflows/*.yaml    -> WorkflowSpec    (Track 1 converts the legacy IMW XML into this)
    configs/*.yaml      -> TestbedSpec     (Track 1 generates the registry from this)
    experiments/*.yaml  -> ExperimentSpec  (Track 3's CLI loads this and starts a runtime)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from orchflow.core.models import Location, ServiceKind


class Spec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# --------------------------------------------------------------------------- workflow


class ServiceRequirement(Spec):
    """One service a task needs ("Gamma" node in the legacy XML)."""

    service_name: str
    kind: ServiceKind
    deployable: bool = False
    count: int = Field(default=1, ge=1)  # how many devices must be bound for this service
    radius_m: float | None = None  # search radius around the task location; None = whole cell
    min_ram_gb: float = 0.0
    min_storage_gb: float = 0.0
    artifact: str | None = None  # artifact name, required iff deployable


class TaskSpec(Spec):
    id: str
    name: str
    location: Location
    requirements: list[ServiceRequirement]
    depends_on: list[str] = []
    period_s: float | None = Field(default=None, gt=0)  # None = runs once
    deadline_s: float | None = Field(default=None, gt=0)  # relative to instance start
    parallel_instances: int = Field(default=1, ge=1)  # e.g. 3 intersections for traffic


class WorkflowSpec(Spec):
    id: str
    name: str
    tasks: list[TaskSpec]

    @model_validator(mode="after")
    def _check_graph(self) -> WorkflowSpec:
        ids = [t.id for t in self.tasks]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate task ids")
        known = set(ids)
        for t in self.tasks:
            missing = set(t.depends_on) - known
            if missing:
                raise ValueError(f"task {t.id} depends on unknown task(s) {sorted(missing)}")
        self.topological_order()  # raises on cycles
        return self

    def task(self, task_id: str) -> TaskSpec:
        for t in self.tasks:
            if t.id == task_id:
                return t
        raise KeyError(task_id)

    def topological_order(self) -> list[TaskSpec]:
        by_id = {t.id: t for t in self.tasks}
        order: list[TaskSpec] = []
        state: dict[str, int] = {}  # 1 = visiting, 2 = done

        def visit(tid: str) -> None:
            if state.get(tid) == 2:
                return
            if state.get(tid) == 1:
                raise ValueError(f"dependency cycle through task {tid}")
            state[tid] = 1
            for dep in by_id[tid].depends_on:
                visit(dep)
            state[tid] = 2
            order.append(by_id[tid])

        for t in self.tasks:
            visit(t.id)
        return order


# --------------------------------------------------------------------------- testbed


class TestbedSpec(Spec):
    """Shape of the testbed. The registry (devices, services, cells) is GENERATED from this
    with a seed, instead of shipping a database dump."""

    __test__ = False  # stop pytest trying to collect this class

    name: str
    n_peers: int = Field(ge=1)
    devices_per_peer: int = Field(ge=1)
    area_center: Location
    cell_radius_m: float = Field(default=100.0, gt=0)
    services_per_device: list[str] = []  # service names devices may offer; generator decides
    registry_seed: int = 0  # seed for the GENERATED layout (independent of run seeds)


# --------------------------------------------------------------------------- experiment


class PluginRef(Spec):
    """Selects a plug-in by name, e.g. {name: nearest} or {name: exponential, params: {...}}."""

    name: str
    params: dict[str, Any] = {}


class Calibration(Spec):
    """Fitted constants. EVERY non-zero value here must be listed in docs/deviations.md."""

    delay_offset_s: float = 0.0  # Table I: delay - redeploy (+3.60 / +4.43 / -0.44 / -0.50)


class ExperimentSpec(Spec):
    name: str
    workflow: Path
    testbed: Path
    runtime: Literal["sim", "live"]
    orchestration: PluginRef  # {name: gom} | {name: lom}
    binding: PluginRef = PluginRef(name="nearest")
    mobility: PluginRef = PluginRef(name="exponential", params={"mean_residence_s": 300})
    network: PluginRef = PluginRef(name="ideal")  # sim: latency model; live: tc netem profile
    work: PluginRef = PluginRef(name="fixed")  # sim: deploy/execute durations
    duration_s: float = Field(default=3600, gt=0)
    seeds: list[int] = [0]
    measured_tasks: list[str] | None = None  # None = all tasks count towards metrics
    calibration: Calibration = Calibration()
    output_dir: Path = Path("results")

    @model_validator(mode="before")
    @classmethod
    def _expand_seeds(cls, data: Any) -> Any:
        # allow `seeds: 100` as shorthand for seeds 0..99
        if isinstance(data, dict) and isinstance(data.get("seeds"), int):
            data = {**data, "seeds": list(range(data["seeds"]))}
        return data


# --------------------------------------------------------------------------- loading

S = TypeVar("S", bound=BaseModel)


def load_yaml(path: str | Path, model: type[S]) -> S:
    """Load and validate a YAML file. Relative paths inside an ExperimentSpec are resolved
    relative to the YAML file's own folder."""
    path = Path(path)
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if model is ExperimentSpec:
        for key in ("workflow", "testbed", "output_dir"):
            if key in data and not Path(data[key]).is_absolute():
                data[key] = str((path.parent / data[key]).resolve())
    return model.model_validate(data)
