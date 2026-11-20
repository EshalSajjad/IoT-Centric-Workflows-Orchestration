"""The plume model, the legacy demo pieces (ALOHA KML, monitor graph, GPS vehicles) and the
dashboard fed by a real live run on the in-memory broker."""

from __future__ import annotations

import asyncio
import math
from pathlib import Path

from test_integration import FAST, FastWork, LeaveOnce

from orchflow.core.models import Location
from orchflow.core.registry import build_registry
from orchflow.core.spec import PluginRef, TestbedSpec, WorkflowSpec, load_yaml
from orchflow.core.testing import tiny_experiment
from orchflow.policies.common import task_peer
from orchflow.runtime.live import LiveAgent, LocalBroker
from orchflow.runtime.live.agent import select_nodes
from orchflow.services.plume import compute_plume, ppm_to_g_m3
from orchflow.viz.graph import workflow_graph
from orchflow.viz.kml import read_aloha_kml, threat_polygon
from orchflow.viz.routes import load_routes
from orchflow.viz.server import Dashboard, render_page
from orchflow.viz.state import DashboardState
from orchflow.viz.vehicles import SPEED_KPH, VehicleSimulator, crosses

ROOT = Path(__file__).resolve().parents[1]
SRC = Location(lat=30.456048, lon=-91.205240)


def reading(v: float) -> dict[str, float]:
    return {"value": v}


def extents(plume: dict) -> dict[str, float]:
    return {f["properties"]["zone"]: f["properties"]["extent_m"]
            for f in plume["features"] if "extent_m" in f["properties"]}


def test_plume_zones_are_nested_and_point_downwind() -> None:
    p = compute_plume({"wind_speed": reading(6), "wind_direction": reading(225),
                       "temperature": reading(25)}, SRC)
    e = extents(p)
    assert e["yellow"] > e["orange"] > e["red"] > 0
    assert 300 < e["yellow"] < 800  # ALOHA demo: 549 yd (502 m)
    # wind FROM the south-west: the plume's far end is north-east of the source
    yellow = next(f for f in p["features"] if f["properties"]["zone"] == "yellow")
    ring = yellow["geometry"]["coordinates"][0]
    lon, lat = ring[len(ring) // 2]
    assert lat > SRC.lat and lon > SRC.lon
    bearing = math.degrees(math.atan2((lon - SRC.lon) * math.cos(math.radians(SRC.lat)),
                                      lat - SRC.lat)) % 360
    assert abs(bearing - 45) < 5


def test_more_wind_dilutes_and_missing_readings_use_defaults() -> None:
    # same stability class (D, > 5 m/s): more wind dilutes faster. (Across classes it isn't
    # monotone: light wind is unstable air, which spreads the gas more.)
    calm = extents(compute_plume({"wind_speed": reading(12)}, SRC))
    windy = extents(compute_plume({"wind_speed": reading(20)}, SRC))
    assert windy["yellow"] < calm["yellow"]
    assert extents(compute_plume({}, SRC))["yellow"] > 0
    assert math.isclose(ppm_to_g_m3(52, 25), 0.166, rel_tol=0.01)  # benzene AEGL-1


def test_aloha_kml_is_legacys_demo_plume() -> None:
    p = read_aloha_kml()
    a = p["properties"]
    assert a["chemical_name"] == "BENZENE" and a["wind"].startswith("9 miles/hour from SW")
    assert a["yellow"].startswith("382 yards") and a["model"] == "ALOHA Gaussian"
    zones = [(f["properties"]["zone"], f["properties"]["confidence_lines"])
             for f in p["features"] if f["geometry"]["type"] == "Polygon"]
    assert zones == [("yellow", True), ("yellow", False), ("orange", False)]
    ring = threat_polygon(p)
    assert ring is not None and len(ring) > 100
    assert all(abs(lat - SRC.lat) < 0.01 and abs(lon - SRC.lon) < 0.01 for lon, lat in ring)


def test_monitor_graph_matches_the_legacy_monitor() -> None:
    g = workflow_graph(load_yaml(ROOT / "workflows" / "imw.yaml", WorkflowSpec))
    at = {n["service"]: (n["col"], n["row"]) for n in g["nodes"]}
    assert at["wind_speed"] == (0, 0) and at["humidity"] == (0, 3)
    assert [s for s, _ in sorted(((s, c) for s, (c, r) in at.items() if r == 0),
                                 key=lambda x: at[x[0]][0])] == [
        "wind_speed", "computation_service_plume", "plume_service", "impose_plume_service",
        "vehicle_status_service", "traffic_flow_service", "re_route_vehicle_service"]
    assert at["camera_feed_intersection_1"] == (4, 1)  # under vehicle status, as in the video
    edges = {(a.split("/")[1], b.split("/")[1]) for a, b in g["edges"]}
    assert ("camera_feed_intersection_1", "traffic_flow_service") in edges
    assert ("vehicle_status_service", "traffic_flow_service") in edges
    assert ("aerosol_concentration", "computation_service_plume") in edges


def test_vehicles_drive_the_legacy_trips_at_40_kph() -> None:
    trips = load_routes()
    assert len(trips) == 3 and trips[0]["from"].startswith("West Baton Rouge Community Center")
    sim = VehicleSimulator(trips)
    assert all(v["position"] is None for v in sim.snapshot(5.0))
    sim.start(10.0)
    car = sim.vehicles[0]
    assert sim.snapshot(10.0)[0]["position"] == car.path[0]
    assert math.isclose(car.travelled_m(sim.elapsed(10.0 + 90.0)), 90.0 * SPEED_KPH / 3.6)
    assert sim.snapshot(10.0 + 3600.0)[0]["status"] == "arrived"
    # legacy's ALOHA plume is clear of all three trips: re-routing changes nothing
    ring = threat_polygon(read_aloha_kml())
    assert ring is not None and not any(crosses(t["path"], ring) for t in trips)
    assert sim.reroute(60.0, ring) == []
    # a zone across a route stops that car
    lat, lon = car.position(500.0)
    box = [(lon - 1e-3, lat - 1e-3), (lon + 1e-3, lat - 1e-3), (lon + 1e-3, lat + 1e-3),
           (lon - 1e-3, lat + 1e-3)]
    assert "vehicle-1: route enters the threat zone: stopped" in sim.reroute(60.0, box)
    assert sim.snapshot(9999.0)[0]["status"] == "blocked"


def test_page_gets_the_map_choice_but_no_key_by_default() -> None:
    page = render_page("google", None).decode()
    assert '{"map": "google", "googleKey": ""}' in page and "/*CONFIG*/" not in page
    assert '"googleKey": "abc"' in render_page("osm", "abc").decode()


def test_dashboard_follows_a_live_run_with_a_redeployment(tmp_path: Path) -> None:
    wf = load_yaml(ROOT / "workflows" / "plume.yaml", WorkflowSpec)
    task = wf.tasks[0].model_copy(update={"period_s": 1.0, "deadline_s": 1.0})
    wf = wf.model_copy(update={"tasks": [task]})
    testbed = TestbedSpec(name="viz", n_peers=3, devices_per_peer=4, area_center=task.location,
                          services_per_device=[r.service_name for r in task.requirements])
    exp = tiny_experiment(runtime="live", duration_s=5.0,
                          orchestration=PluginRef(name="gom", params=FAST),
                          binding=PluginRef(name="nearest"))
    reg = build_registry(testbed, wf)
    req = next(r for r in task.requirements if r.deployable)
    target = min(reg.candidates(req, task.location, cell_id=task_peer(reg, task).cell_id),
                 key=lambda d: (d.location.distance_m(task.location), d.id)).id

    broker = LocalBroker()
    state = DashboardState(wf, build_registry(testbed, wf), heartbeat_s=FAST["heartbeat_s"],
                           plume_mode="computed")
    dash = Dashboard(state, broker.transport())
    nodes = select_nodes("all", reg)
    agent = LiveAgent(experiment=exp, workflow=wf, registry=reg, nodes=nodes,
                      transport=broker.transport(), run_dir=tmp_path,
                      name="all", work=FastWork(), mobility=LeaveOnce(target),
                      ready_timeout_s=5)

    async def scenario() -> None:
        listener = asyncio.create_task(dash.listen(nodes))
        await asyncio.sleep(0.05)
        await agent.run()
        listener.cancel()

    asyncio.run(scenario())
    import time
    frozen = dash.now()
    time.sleep(0.05)
    assert dash.now() == frozen > 0  # the run stopped: the dashboard shows it as it ended
    late = state.snapshot(dash.now())
    assert sum(d["state"] == "away" for d in late["devices"]) < len(late["devices"]) / 2
    snap = state.snapshot()
    t = snap["tasks"][0]
    assert t["ok"] >= 3 and t["failed"] == 0
    assert set(t["readings"]) >= {"wind_speed", "wind_direction", "temperature"}
    assert snap["plume"] is not None and extents(snap["plume"])["yellow"] > 0
    assert snap["rebinds"] == {"started": 1, "done": 1}
    texts = [e["text"] for e in snap["log"]]
    assert any("has left" in x and target in x for x in texts)
    assert any("plume imposed" in x for x in texts)
    states = {d["id"]: d["state"] for d in snap["devices"]}
    assert states[target] == "away"
    assert "serving" in states.values()
    boxes = {n["service"]: n["state"] for n in snap["monitor"]}
    assert "device leave" not in boxes.values()  # redeployment done: no box left red
    assert snap["vehicles"] == []  # the plume workflow has no vehicle_status_service


def test_monitor_colours_and_aloha_plume_follow_the_imw() -> None:
    from orchflow.core.messages import Message, MsgKind
    from orchflow.core.models import COORDINATOR_ID
    from orchflow.viz.state import EXEC_HOLD_S

    wf = load_yaml(ROOT / "workflows" / "imw.yaml", WorkflowSpec)
    testbed = TestbedSpec(name="viz", n_peers=2, devices_per_peer=2, area_center=SRC,
                          services_per_device=["wind_speed"])
    state = DashboardState(wf, build_registry(testbed, wf))
    state.on_control({"type": "start"})
    coord, peer, dev = COORDINATOR_ID, "peer-01", "dev-0001"

    def boxes(t: float | None = None) -> dict[str, str]:
        return {n["service"]: n["state"] for n in state.monitor(t)}

    def send(kind: MsgKind, src: str, dst: str, t: float, **kw: object) -> None:
        state.on_message(Message(kind=kind, src=src, dst=dst, **kw), t)  # type: ignore[arg-type]

    def run(task: str, t: float) -> None:
        send(MsgKind.TASK_ASSIGN, coord, peer, t, task_id=task, instance=1)
        send(MsgKind.TASK_RESULT, peer, coord, t, task_id=task, instance=1, payload={"ok": True})

    assert set(boxes().values()) == {"waiting for input"}
    # setup: the coordinator binds task 1 -> its boxes are ready for deployment
    send(MsgKind.TASK_ASSIGN, coord, peer, 0.5, task_id="1", payload={"bindings": {}})
    b = boxes()
    assert b["wind_speed"] == b["plume_service"] == "ready for deployment"
    assert b["impose_plume_service"] == "waiting for input"
    # instance 1: the peer asks the wind sensor; only that box is in execution
    send(MsgKind.TASK_ASSIGN, coord, peer, 1.0, task_id="1", instance=1, payload={"slot": 0})
    send(MsgKind.DATA_REQUEST, peer, dev, 1.0, task_id="1", instance=1,
         payload={"key": "1/0/wind_speed/0"})
    b = boxes(1.0)
    assert b["wind_speed"] == "in execution" and b["plume_service"] == "waiting for input"
    send(MsgKind.TASK_RESULT, dev, peer, 1.2, task_id="1", instance=1,
         payload={"key": "1/0/wind_speed/0", "ok": True})
    assert boxes(1.2)["wind_speed"] == "in execution"  # held so the page can show it
    assert boxes(1.2 + EXEC_HOLD_S + 0.1)["wind_speed"] == "waiting for input"
    send(MsgKind.TASK_RESULT, peer, coord, 1.3, task_id="1", instance=1, payload={"ok": True})
    assert state.plume is None  # legacy imposes the plume in the impose_plume_service task
    # a device leaves: red until a replacement is picked, green while it is deployed
    send(MsgKind.REDEPLOY_START, peer, coord, 2.0, task_id="1", rebind_id="rb-1",
         payload={"key": "1/0/plume_service/0"})
    assert boxes(2.0)["plume_service"] == "device leave"
    send(MsgKind.REBIND_REPLY, coord, peer, 4.0, task_id="1", rebind_id="rb-1",
         payload={"device_id": "dev-0002"})
    assert boxes(4.0)["plume_service"] == "ready for deployment"
    send(MsgKind.REDEPLOY_DONE, peer, coord, 6.0, task_id="1", rebind_id="rb-1", payload={})
    assert boxes(6.0 + EXEC_HOLD_S)["plume_service"] == "waiting for input"
    # a fast redeployment still shows red, then green, for EXEC_HOLD_S each
    send(MsgKind.REDEPLOY_START, peer, coord, 10.0, task_id="1", rebind_id="rb-2",
         payload={"key": "1/0/humidity/0"})
    send(MsgKind.REDEPLOY_DONE, peer, coord, 10.1, task_id="1", rebind_id="rb-2", payload={})
    assert boxes(10.2)["humidity"] == "device leave"
    assert boxes(10.0 + EXEC_HOLD_S + 0.1)["humidity"] == "ready for deployment"
    assert boxes(10.0 + 2 * EXEC_HOLD_S + 0.1)["humidity"] == "waiting for input"
    # a redeployment whose "done" never comes ends when the service answers again
    send(MsgKind.REDEPLOY_START, peer, coord, 20.0, task_id="1", rebind_id="rb-3",
         payload={"key": "1/0/temperature/0"})
    send(MsgKind.REBIND_REPLY, coord, peer, 21.0, task_id="1", rebind_id="rb-3",
         payload={"device_id": "dev-0003"})
    assert boxes(30.0)["temperature"] == "ready for deployment"
    send(MsgKind.TASK_RESULT, "dev-0003", peer, 31.0, task_id="1",
         payload={"key": "1/0/temperature/0", "ok": True})
    assert boxes(31.0 + EXEC_HOLD_S + 0.1)["temperature"] == "waiting for input"
    # a cloud service is in execution while its task's instance runs on the coordinator
    send(MsgKind.TASK_ASSIGN, coord, peer, 4.0, task_id="2", instance=1)
    assert boxes(4.0)["impose_plume_service"] == "in execution"
    send(MsgKind.TASK_RESULT, peer, coord, 4.1, task_id="2", instance=1, payload={"ok": True})
    assert boxes(4.1 + EXEC_HOLD_S + 0.1)["impose_plume_service"] == "waiting for input"
    assert state.plume is not None and state.plume["properties"]["chemical_name"] == "BENZENE"
    run("3", 5.0)  # vehicle_status_service starts the GPS simulator
    cars = state.snapshot(5.0 + 60.0)["vehicles"]
    assert len(cars) == 3 and all(c["position"] and c["status"] == "driving" for c in cars)
    run("4", 6.0)
    run("5", 7.0)
    assert any("no vehicle route enters the threat zone" in e["text"] for e in state.log)
    static = state.static()
    assert len(static["routes"]) == 3 and static["aloha"]["chemical_name"] == "BENZENE"
