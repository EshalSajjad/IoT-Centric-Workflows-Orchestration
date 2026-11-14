"""Track 1: legacy workflow XML importer and the committed workflow YAMLs."""

from __future__ import annotations

from pathlib import Path

import pytest
from policy_net import Net

from orchflow.core.events import DeviceBound, RebindStarted, TaskInstanceEnded, TaskInstanceStarted
from orchflow.core.models import ServiceKind
from orchflow.core.plugins import create
from orchflow.core.registry import build_registry
from orchflow.core.spec import PluginRef, TestbedSpec, WorkflowSpec, load_yaml
from orchflow.core.testing import tiny_experiment
from orchflow.importers.legacy_xml import extract, parse_legacy_workflow, to_yaml

ROOT = Path(__file__).resolve().parents[1]
XML = ROOT / "tests" / "fixtures" / "legacy_imw.xml"  # copy of the legacy IMW model


def body(text: str) -> str:
    return "".join(line for line in text.splitlines(keepends=True) if not line.startswith("#"))


def test_imports_the_legacy_imw_workflow() -> None:
    wf = parse_legacy_workflow(XML)
    assert [t.id for t in wf.tasks] == ["1", "2", "3", "4", "5"]
    # chain built from cross-task <interaction>s: 7->8, 8->9, 9->11, 11->12
    assert {t.id: t.depends_on for t in wf.tasks} == {
        "1": [], "2": ["1"], "3": ["2"], "4": ["3"], "5": ["4"]}
    plume = wf.task("1")
    assert [r.service_name for r in plume.requirements] == [
        "wind_speed", "wind_direction", "temperature", "humidity", "aerosol_concentration",
        "computation_service_plume", "plume_service"]
    deployable = {r.service_name: r.artifact for t in wf.tasks for r in t.requirements
                  if r.deployable}
    assert deployable == {"plume_service": "data_integration_service.jar",
                          "traffic_flow_service": "Traffic_Flow_Finder.jar"}
    assert wf.task("2").requirements[0].kind == ServiceKind.CLOUD
    # the legacy device query ignores radius and hardware requirements; so do we
    assert all(r.radius_m is None and r.min_ram_gb == 0 for t in wf.tasks for r in t.requirements)


def test_bad_xml_is_rejected() -> None:
    with pytest.raises(ValueError, match="Service_Type"):
        parse_legacy_workflow("<Workflow><Task id='1'><Gamma id='1'><Service_Name>x"
                              "</Service_Name><Service_Type>quantum</Service_Type>"
                              "<Deployable>non_deployable</Deployable><Location><Latitude>1</Latitude>"
                              "<Longitude>1</Longitude></Location></Gamma></Task></Workflow>")
    with pytest.raises(ValueError, match="Location"):
        parse_legacy_workflow("<Workflow><Task id='1'><Gamma id='1'><Service_Name>x"
                              "</Service_Name><Service_Type>sensor</Service_Type>"
                              "<Deployable>non_deployable</Deployable></Gamma></Task></Workflow>")


def test_committed_workflow_yamls_match_the_importer() -> None:
    wf = parse_legacy_workflow(XML)
    expected = {
        "imw.yaml": wf,
        "plume.yaml": extract(wf, ["1"], workflow_id="plume", name="Plume modelling (IMW task 1)",
                              period_s=60, deadline_s=58),
        "traffic.yaml": extract(wf, ["4"], workflow_id="traffic",
                                name="Traffic monitoring (IMW task 4, 3 intersections)",
                                period_s=60, deadline_s=40, parallel_instances=3),
    }
    for fname, spec in expected.items():
        committed = (ROOT / "workflows" / fname).read_text(encoding="utf-8")
        assert body(committed) == to_yaml(spec), f"workflows/{fname} is stale; regenerate it"
        assert load_yaml(ROOT / "workflows" / fname, WorkflowSpec) == spec


@pytest.mark.parametrize(("mode", "workload", "cost"),
                         [("gom", "plume", 8), ("lom", "plume", 6),
                          ("gom", "traffic", 8), ("lom", "traffic", 6)])
def test_full_testbed_hour_with_one_departure(mode: str, workload: str, cost: int) -> None:
    """The real workflows on the 20 x 20 testbed for 60 minutes: everything binds, every
    instance finishes, and one departure costs exactly one redeployment."""
    wf = load_yaml(ROOT / "workflows" / f"{workload}.yaml", WorkflowSpec)
    reg = build_registry(load_yaml(ROOT / "configs" / "testbed_20x20.yaml", TestbedSpec), wf)
    exp = tiny_experiment(orchestration=PluginRef(name=mode), binding=PluginRef(name="nearest"))
    net = Net(create("orchestration", mode), reg, wf, exp)
    net.run(60)
    fog = wf.tasks[0].requirements[-1].service_name  # the deployable service
    first = min(e.device_id for e in net.of_type(DeviceBound)
                if e.rebind_id is None and e.service_name == fog)
    net.depart(first, at=600)
    net.run(3600)
    assert len(net.of_type(RebindStarted)) == 1
    assert len([m for m in net.sent if m.is_overhead]) == cost
    started = net.of_type(TaskInstanceStarted)
    ended = net.of_type(TaskInstanceEnded)
    assert all(e.ok for e in ended)
    assert len(started) - len(ended) <= wf.tasks[0].parallel_instances  # only the last tick open
