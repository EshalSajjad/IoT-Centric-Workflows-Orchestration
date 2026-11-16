"""Track 1: registry, binding policies, and the GOM / LOM protocols."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from policy_net import Net

from orchflow.core.events import (
    DeviceBound,
    RebindEnded,
    RebindStarted,
    TaskInstanceEnded,
    TaskInstanceStarted,
)
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import COORDINATOR_ID, ServiceKind
from orchflow.core.plugins import available, create
from orchflow.core.registry import build_registry
from orchflow.core.spec import (
    PluginRef,
    ServiceRequirement,
    TaskSpec,
    TestbedSpec,
    WorkflowSpec,
    load_yaml,
)
from orchflow.core.testing import CENTER, RecordingContext, tiny_experiment
from orchflow.policies.common import bkey

ROOT = Path(__file__).resolve().parents[1]

GOM_REBIND = [MsgKind.REDEPLOY_START, MsgKind.DEVICE_LEFT, MsgKind.REBIND_REPLY,
              MsgKind.DEPLOY, MsgKind.TASK_ASSIGN, MsgKind.START_SERVICE,
              MsgKind.TASK_RESULT, MsgKind.REDEPLOY_DONE]
LOM_REBIND = [MsgKind.DEVICE_LEFT, MsgKind.REDEPLOY_START, MsgKind.DEVICE_LEFT,
              MsgKind.REDEPLOY_DONE, MsgKind.START_SERVICE, MsgKind.TASK_RESULT]


def make_testbed(n_peers: int = 2, per_peer: int = 4) -> TestbedSpec:
    return TestbedSpec(name="t", n_peers=n_peers, devices_per_peer=per_peer,
                       area_center=CENTER, cell_radius_m=100,
                       services_per_device=["wind_speed", "plume_service"])


def plume_workflow(parallel: int = 1, wind_count: int = 1) -> WorkflowSpec:
    return WorkflowSpec(id="p", name="plume-like", tasks=[TaskSpec(
        id="1", name="plume", location=CENTER, period_s=60, deadline_s=58,
        parallel_instances=parallel,
        requirements=[
            ServiceRequirement(service_name="wind_speed", kind=ServiceKind.SENSOR,
                               count=wind_count),
            ServiceRequirement(service_name="plume_service", kind=ServiceKind.FOG,
                               deployable=True, artifact="data_integration_service.jar"),
        ])])


def make_net(mode: str, n_peers: int = 2, per_peer: int = 4, parallel: int = 1,
             seed: int = 0, wind_count: int = 1, **params: object) -> Net:
    wf = plume_workflow(parallel, wind_count)
    reg = build_registry(make_testbed(n_peers, per_peer), wf)
    ref = PluginRef(name=mode, params=dict(params))
    exp = tiny_experiment(orchestration=ref, binding=PluginRef(name="nearest"))
    return Net(create("orchestration", ref), reg, wf, exp, seed=seed)


def initial_device(net: Net, service: str) -> str:
    return next(e.device_id for e in net.of_type(DeviceBound)
                if e.service_name == service and e.rebind_id is None)


def rebind_for(net: Net, service: str) -> RebindStarted:
    [rb] = [e for e in net.of_type(RebindStarted) if e.service_name == service]
    return rb


# =========================================================================== registry


def test_registry_is_generated_deterministically_from_the_testbed_yaml() -> None:
    spec = load_yaml(ROOT / "configs" / "testbed_20x20.yaml", TestbedSpec)
    a, b = build_registry(spec), build_registry(spec)
    assert len(a.peers()) == 20 and len(a.devices()) == 400 and len(a.cells()) == 20
    assert [d.location for d in a.devices()] == [d.location for d in b.devices()]
    cells = {c.id: c for c in a.cells()}
    for d in a.devices():  # every device lies inside its own cell
        assert d.location.distance_m(cells[d.cell_id].center) <= cells[d.cell_id].radius_m
    assert {s.name for s in a.services_of("dev-0001")} == set(spec.services_per_device)


def test_registry_takes_service_kinds_from_the_workflow() -> None:
    reg = build_registry(make_testbed(), plume_workflow())
    svc = {s.name: s for s in reg.services_of("dev-0001")}
    assert svc["plume_service"].deployable and svc["plume_service"].artifact is not None
    assert svc["plume_service"].artifact.name == "data_integration_service.jar"
    assert svc["wind_speed"].kind == ServiceKind.SENSOR and not svc["wind_speed"].deployable


def test_candidates_respect_cell_exclusion_and_availability() -> None:
    reg = build_registry(make_testbed(), plume_workflow())
    req = ServiceRequirement(service_name="wind_speed", kind=ServiceKind.SENSOR)
    in_cell = reg.candidates(req, CENTER, cell_id="cell-01")
    assert [d.id for d in in_cell] == ["dev-0001", "dev-0002", "dev-0003", "dev-0004"]
    reg.set_available("dev-0002", False)
    got = reg.candidates(req, CENTER, cell_id="cell-01", exclude=frozenset({"dev-0003"}))
    assert [d.id for d in got] == ["dev-0001", "dev-0004"]


# =========================================================================== binding


def test_binding_plugins_are_registered() -> None:
    assert {"nearest", "qos"} <= set(available("binding"))
    assert {"gom", "lom"} <= set(available("orchestration"))


def test_nearest_picks_the_closest_device_and_breaks_ties_by_id() -> None:
    reg = build_registry(make_testbed(1, 6), plume_workflow())
    req = ServiceRequirement(service_name="wind_speed", kind=ServiceKind.SENSOR)
    cands = reg.candidates(req, CENTER)
    best = create("binding", "nearest").select(req, cands, CENTER, reg, np.random.default_rng())
    assert best.id == min(cands, key=lambda d: d.location.distance_m(CENTER)).id
    twin = cands[0].model_copy(update={"id": "dev-0000"})  # same spot, smaller id
    pick = create("binding", "nearest").select(req, [cands[0], twin], cands[0].location, reg,
                                               np.random.default_rng())
    assert pick.id == "dev-0000"


def test_qos_prefers_the_lowest_score() -> None:
    reg = build_registry(make_testbed(1, 3), plume_workflow())
    req = ServiceRequirement(service_name="wind_speed", kind=ServiceKind.SENSOR)
    devs = list(reg.candidates(req, CENTER))
    slow = devs[0].model_copy(update={"specs": devs[0].specs.model_copy(
        update={"response_time_s": 5.0})})
    pick = create("binding", PluginRef(name="qos")).select(
        req, [slow, devs[1]], CENTER, reg, np.random.default_rng())
    assert pick.id == devs[1].id


# =========================================================================== steady state


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_without_departures_every_instance_completes_and_nothing_is_overhead(mode: str) -> None:
    net = make_net(mode)
    net.run(600)
    started = net.of_type(TaskInstanceStarted)
    ended = {e.instance: e for e in net.of_type(TaskInstanceEnded)}
    assert len(started) == 10  # ready at t=3 (one 3 s deploy), then every 60 s up to 600
    assert all(ended[s.instance].ok and ended[s.instance].t <= s.deadline_at for s in started)
    assert not [m for m in net.sent if m.is_overhead]


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_parallel_slots_get_their_own_devices(mode: str) -> None:
    net = make_net(mode, parallel=3)
    net.run(30)
    bound = [e.device_id for e in net.of_type(DeviceBound) if e.service_name == "plume_service"]
    assert len(bound) == 3 and len(set(bound)) == 3
    assert len(net.of_type(TaskInstanceStarted)) == 3  # 3 instances per tick


# =========================================================================== redeployment


@pytest.mark.parametrize(("mode", "expected"), [("gom", GOM_REBIND), ("lom", LOM_REBIND)])
def test_a_departure_triggers_the_legacy_message_sequence(mode: str,
                                                          expected: list[MsgKind]) -> None:
    net = make_net(mode)
    net.run(30)
    gone = initial_device(net, "plume_service")
    net.depart(gone, at=100)
    net.run(600)

    rb = rebind_for(net, "plume_service")
    assert rb.failed_device_id == gone
    assert net.kinds(net.rebind_messages(rb.rebind_id)) == expected
    [end] = [e for e in net.of_type(RebindEnded) if e.rebind_id == rb.rebind_id]
    assert end.ok and end.new_device_id not in (None, gone)
    assert end.node == rb.node  # redeploy time measured on ONE node's clock


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_with_monitor_all_a_sensor_rebind_costs_the_same(mode: str) -> None:
    """Non-legacy option: every binding watched; a sensor is rebound with the same messages
    (just no artifact upload)."""
    net = make_net(mode, monitor="all")
    net.run(30)
    net.depart(initial_device(net, "wind_speed"), at=100)
    net.run(600)
    rb = rebind_for(net, "wind_speed")
    assert len(net.rebind_messages(rb.rebind_id)) == {"gom": 8, "lom": 6}[mode]


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_instances_waiting_on_the_departed_device_finish_after_the_rebind(mode: str) -> None:
    net = make_net(mode)
    net.run(30)
    net.depart(initial_device(net, "plume_service"), at=100)
    net.run(900)
    started = net.of_type(TaskInstanceStarted)
    ended = {e.instance: e for e in net.of_type(TaskInstanceEnded)}
    assert {s.instance for s in started} == set(ended)  # nothing is lost
    assert all(e.ok for e in ended.values())
    late = [s for s in started if ended[s.instance].t > (s.deadline_at or 0)]
    assert late  # the outage made at least one instance miss its deadline


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_replacement_stays_in_the_task_cell(mode: str) -> None:
    net = make_net(mode, n_peers=4)
    net.run(30)
    gone = initial_device(net, "plume_service")
    net.depart(gone, at=100)
    net.run(600)
    ends = net.of_type(RebindEnded)  # one per service the departed device was serving
    assert ends and all(e.ok for e in ends)
    reg = net.registry
    for end in ends:
        assert reg.device(end.new_device_id or "").cell_id == reg.device(gone).cell_id


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_no_replacement_fails_the_waiting_instances(mode: str) -> None:
    net = make_net(mode, n_peers=1, per_peer=1)  # nobody to take over
    net.run(30)
    net.depart("dev-0001", at=100)
    net.run(600)
    ends = net.of_type(RebindEnded)
    assert ends and not any(e.ok for e in ends)
    assert any(not e.ok for e in net.of_type(TaskInstanceEnded))


def test_gom_coordinator_never_repicks_the_departed_device() -> None:
    """Legacy selectAlternateDevice could hand the job straight back to the device that just
    left. Here the registry still lists it as available, so only the exclusion stops it."""
    wf = plume_workflow()
    reg = build_registry(make_testbed(1, 3), wf)
    exp = tiny_experiment(orchestration=PluginRef(name="gom"), binding=PluginRef(name="nearest"))
    coord = create("orchestration", "gom").coordinator()
    ctx = RecordingContext(COORDINATOR_ID, reg, exp, wf)
    coord.on_start(ctx)
    key = bkey("1", 0, "wind_speed")
    gone = next(e.device_id for e in ctx.events
                if isinstance(e, DeviceBound) and e.service_name == "wind_speed")
    ctx.sent.clear()
    coord.on_message(ctx, Message(kind=MsgKind.DEVICE_LEFT, src="peer-01", dst=COORDINATOR_ID,
                                  task_id="1", rebind_id="rb-x",
                                  payload={"key": key, "device_id": gone}))
    [reply] = [m for m in ctx.sent if m.kind == MsgKind.REBIND_REPLY]
    assert reply.payload["device_id"] not in (None, gone)
    assert len(ctx.overhead_messages()) == 4  # the coordinator's share of the 8


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_runs_are_reproducible(mode: str) -> None:
    def trace() -> list[tuple[float, str, str, str]]:
        net = make_net(mode)
        net.run(30)
        net.depart(initial_device(net, "plume_service"), at=100)
        net.run(600)
        return [(m.sent_at or 0, m.kind, m.src, m.dst) for m in net.sent]

    assert trace() == trace()


def test_protocol_params_are_validated() -> None:
    with pytest.raises(ValueError, match="monitor"):
        create("orchestration", PluginRef(name="gom", params={"monitor": "some"}))
    proto = create("orchestration", PluginRef(name="lom", params={"heartbeat_s": 10}))
    assert proto.params.heartbeat_s == 10


# =========================================================================== legacy watching


@pytest.mark.parametrize(("mode", "cost"), [("gom", 8), ("lom", 6)])
def test_sensors_on_the_departed_device_move_with_the_redeployed_service(mode: str,
                                                                          cost: int) -> None:
    """Legacy rewrites the departed device's IP across the whole task, so ONE redeployment
    also moves the sensor it was serving -- no second rebind, no stalled task."""
    net = make_net(mode)
    net.run(30)
    gone = initial_device(net, "plume_service")
    assert initial_device(net, "wind_speed") == gone  # nearest device serves both
    net.depart(gone, at=100)
    net.run(900)
    [rb] = net.of_type(RebindStarted)
    assert rb.service_name == "plume_service"
    assert len([m for m in net.sent if m.is_overhead]) == cost
    [end] = net.of_type(RebindEnded)
    late_reads = [m for m in net.sent if m.kind == MsgKind.DATA_REQUEST and (m.sent_at or 0) > 300]
    assert late_reads and all(m.dst == end.new_device_id for m in late_reads)
    ended = net.of_type(TaskInstanceEnded)
    assert len(ended) == len(net.of_type(TaskInstanceStarted)) and all(e.ok for e in ended)


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_by_default_only_devices_with_a_deployment_are_watched(mode: str) -> None:
    net = make_net(mode, wind_count=2)  # the 2nd wind_speed device only runs a sensor
    net.run(30)
    sensor_only = {e.device_id for e in net.of_type(DeviceBound)} - {
        initial_device(net, "plume_service")}
    [dev] = sensor_only
    net.depart(dev, at=100)
    net.run(600)
    assert not net.of_type(RebindStarted)  # legacy never notices a departed sensor
