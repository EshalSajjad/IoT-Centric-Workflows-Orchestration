"""Tests for the shared contracts. If these break, someone changed the contract."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from orchflow.core import plugins
from orchflow.core.events import MessageSent, TaskInstanceEnded, read_events, write_events
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import Location, ServiceKind
from orchflow.core.spec import (
    ExperimentSpec,
    ServiceRequirement,
    TaskSpec,
    TestbedSpec,
    WorkflowSpec,
    load_yaml,
)
from orchflow.core.testing import (
    CENTER,
    RecordingContext,
    sample_events,
    tiny_experiment,
    tiny_registry,
    tiny_workflow,
)

ROOT = Path(__file__).resolve().parents[1]
SENSOR = ServiceKind.SENSOR


# ------------------------------------------------------------------ models / specs


def test_distance_is_metres() -> None:
    assert 110 < Location(lat=0, lon=0).distance_m(Location(lat=0, lon=0.001)) < 112


def test_workflow_rejects_cycles_and_unknown_deps() -> None:
    req = [ServiceRequirement(service_name="x", kind=SENSOR)]
    with pytest.raises(ValidationError, match="cycle"):
        WorkflowSpec(id="w", name="w", tasks=[
            TaskSpec(id="a", name="a", location=CENTER, requirements=req, depends_on=["b"]),
            TaskSpec(id="b", name="b", location=CENTER, requirements=req, depends_on=["a"]),
        ])
    with pytest.raises(ValidationError, match="unknown"):
        WorkflowSpec(id="w", name="w", tasks=[
            TaskSpec(id="a", name="a", location=CENTER, requirements=req, depends_on=["zz"]),
        ])


def test_topological_order() -> None:
    assert [t.id for t in tiny_workflow().topological_order()] == ["1", "2"]


def test_example_yaml_files_validate() -> None:
    wf = load_yaml(ROOT / "workflows/example_tiny.yaml", WorkflowSpec)
    assert wf.task("1").deadline_s == 58
    tb = load_yaml(ROOT / "configs/testbed_20x20.yaml", TestbedSpec)
    assert tb.n_peers * tb.devices_per_peer == 400
    ex = load_yaml(ROOT / "experiments/example_echo.yaml", ExperimentSpec)
    assert ex.seeds == [0, 1, 2]
    assert ex.workflow.is_absolute() and ex.workflow.exists()


def test_specs_reject_typos() -> None:
    with pytest.raises(ValidationError):
        tiny_experiment(duraton_s=10)  # extra="forbid" catches misspelt keys


# ------------------------------------------------------------------ messages / events


def test_message_roundtrip_and_overhead_flag() -> None:
    m = Message(kind=MsgKind.REBIND_REQUEST, src="peer-01", dst="coord", rebind_id="rb-1")
    assert Message.from_json(m.to_json()) == m
    assert m.is_overhead
    assert not Message(kind=MsgKind.HEARTBEAT, src="dev-0001", dst="peer-01").is_overhead


def test_event_log_roundtrip(tmp_path: Path) -> None:
    events = sample_events()
    write_events(tmp_path / "events.jsonl", events)
    assert list(read_events(tmp_path / "events.jsonl")) == events


def test_sample_events_match_documented_answers() -> None:
    ev = sample_events()
    sent = [e for e in ev if isinstance(e, MessageSent)]
    assert len(sent) == 14
    assert sum(1 for e in sent if e.rebind_id) == 8
    assert sum(1 for e in ev if isinstance(e, TaskInstanceEnded)) == 3


# ------------------------------------------------------------------ registry fake


def test_candidates_respect_exclude_availability_and_cell() -> None:
    reg = tiny_registry()
    req = ServiceRequirement(service_name="wind_speed", kind=SENSOR)
    all_ids = {d.id for d in reg.candidates(req, CENTER)}
    assert len(all_ids) == 6
    reg.set_available("dev-0001", False)
    got = {d.id for d in reg.candidates(req, CENTER, exclude=frozenset({"dev-0002"}))}
    assert got == all_ids - {"dev-0001", "dev-0002"}
    assert {d.cell_id for d in reg.candidates(req, CENTER, cell_id="cell-02")} == {"cell-02"}


# ------------------------------------------------------------------ plug-ins


def test_plugins_registered_and_created() -> None:
    assert "echo" in plugins.available("orchestration")
    proto = plugins.create("orchestration", tiny_experiment().orchestration)
    assert proto.name == "echo" and proto.period_s == 10
    with pytest.raises(KeyError, match="available"):
        plugins.create("binding", "does-not-exist")


# ------------------------------------------------------------------ echo protocol via fake ctx


def test_echo_protocol_full_cycle_with_recording_context() -> None:
    """Shows how Track 1 tests logic without any runtime: drive the callbacks by hand."""
    reg, exp, wf = tiny_registry(), tiny_experiment(), tiny_workflow()
    proto = plugins.create("orchestration", exp.orchestration)
    coord, peer = proto.coordinator(), proto.peer(reg.peer("peer-01"))
    dev = proto.device(reg.device("dev-0001"))
    c_ctx = RecordingContext("coord", reg, exp, wf)
    p_ctx = RecordingContext("peer-01", reg, exp, wf)
    d_ctx = RecordingContext("dev-0001", reg, exp, wf)

    coord.on_start(c_ctx)
    c_ctx.fire_timer(coord, "tick")
    assign = next(m for m in c_ctx.sent if m.dst == "peer-01")
    peer.on_message(p_ctx, assign)
    req = p_ctx.sent[-1]
    assert req.kind == MsgKind.DATA_REQUEST and req.dst == "dev-0001"
    dev.on_message(d_ctx, req)
    d_ctx.finish_work(dev, d_ctx.work[0].work_id)
    peer.on_message(p_ctx, d_ctx.sent[-1])
    coord.on_message(c_ctx, p_ctx.sent[-1])

    ended = [e for e in c_ctx.events if isinstance(e, TaskInstanceEnded)]
    assert ended and ended[0].ok
    assert "tick" in c_ctx.timers  # rescheduled for the next period


def test_rng_is_deterministic_per_seed_and_node() -> None:
    reg, exp, wf = tiny_registry(), tiny_experiment(), tiny_workflow()
    a = RecordingContext("dev-0001", reg, exp, wf, seed=7).rng.random()
    b = RecordingContext("dev-0001", reg, exp, wf, seed=7).rng.random()
    c = RecordingContext("dev-0002", reg, exp, wf, seed=7).rng.random()
    assert a == b != c


def test_cli_lists_plugins(capsys: pytest.CaptureFixture[str]) -> None:
    from orchflow.cli import main

    assert main(["plugins"]) == 0
    assert "echo" in capsys.readouterr().out
