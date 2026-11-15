"""Track 2: deterministic SimPy runtime and mobility/network/work plug-ins.

Importing the simulator registers its plug-ins without modifying shared core code.
The live runtime can import ``orchflow.runtime.sim.mobility`` to use the same model.

``run_seed`` is the entry point the CLI (`orchflow run`, Track 3) calls.
"""

from __future__ import annotations

from pathlib import Path

from orchflow.core.spec import ExperimentSpec, TestbedSpec, WorkflowSpec, load_yaml


def run_seed(experiment: ExperimentSpec, seed: int, run_dir: str | Path) -> Path:
    """One seed of an experiment: fresh registry (Track 1) -> SimPy run -> events.jsonl."""
    from orchflow.core.registry import build_registry
    from orchflow.runtime.sim.env import run_simulation

    workflow = load_yaml(experiment.workflow, WorkflowSpec)
    testbed = load_yaml(experiment.testbed, TestbedSpec)
    path = Path(run_dir) / "events.jsonl"
    run_simulation(experiment, workflow, build_registry(testbed, workflow), seed=seed,
                   event_log_path=path)
    return path


__all__ = ["run_seed"]
