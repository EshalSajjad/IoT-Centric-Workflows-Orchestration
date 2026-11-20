"""What the dashboard knows, built only from the messages of a run (no access to node state).

It stands in for legacy's three demo windows:

* the **workflow monitor** (``MqttWeb/web-page-monitor.htm``): one box per service, coloured
  waiting for input / ready for deployment / in execution / device leave, following each
  service's current instance (see ``monitor``);
* the **plume map** (``plume_display.html``): the ALOHA threat zones on the map, imposed when
  the impose_plume_service task runs, plus the "Aloha Plume Model" chart;
* the **vehicle map** (``map_display.html`` / re-route page): the GPS simulator's cars on
  their red routes, started by the vehicle_status_service task.

On top of legacy it shows the devices (present / away / serving), the cells and an event log.
The dashboard is a passive observer: it reads every node's inbox and never sends anything,
so it can't change what is measured.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from orchflow.core.interfaces import Registry
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import COORDINATOR_ID, ServiceKind
from orchflow.core.spec import WorkflowSpec
from orchflow.policies.common import bindable, parse_bkey
from orchflow.services.plume import compute_plume
from orchflow.viz.graph import node_id, workflow_graph
from orchflow.viz.kml import read_aloha_kml, threat_polygon
from orchflow.viz.vehicles import VehicleSimulator

PLUME_INPUTS = {"wind_speed", "wind_direction"}
# legacy's coordinator keys its cloud steps on these service names
# (Workflow_Coordinator_Main: contains("vehicle_status_service") ...)
PLUME_SERVICE = "plume_service"
IMPOSE_SERVICE = "impose_plume_service"
VEHICLE_SERVICE = "vehicle_status_service"
REROUTE_SERVICE = "re_route_vehicle_service"
PLUME_MODES = ("aloha", "computed")
# a box stays grey at least this long after its work ends, so a service that answers within
# milliseconds still shows "in execution" on a page that polls every second (display only)
EXEC_HOLD_S = 1.5


class DashboardState:
    def __init__(self, workflow: WorkflowSpec, registry: Registry, *,
                 heartbeat_s: float = 60.0, log_size: int = 60, plume_mode: str = "aloha",
                 vehicles: VehicleSimulator | None = None) -> None:
        if plume_mode not in PLUME_MODES:
            raise ValueError(f"plume_mode must be one of {PLUME_MODES}")
        self.workflow, self.registry, self.plume_mode = workflow, registry, plume_mode
        self.away_after_s = heartbeat_s * 1.5
        self.t = 0.0
        self.running = False
        self.log: deque[dict[str, Any]] = deque(maxlen=log_size)
        self.devices: dict[str, dict[str, Any]] = {
            d.id: {"id": d.id, "peer": d.peer_id, "lat": d.location.lat, "lon": d.location.lon,
                   "last_seen": None, "serving": [], "left": False}
            for d in registry.devices()}
        self.tasks: dict[str, dict[str, Any]] = {
            t.id: {"id": t.id, "name": t.name, "lat": t.location.lat, "lon": t.location.lon,
                   "depends_on": list(t.depends_on), "status": "waiting for setup",
                   "started": 0, "ok": 0, "failed": 0, "readings": {}}
            for t in workflow.tasks}
        self.rebinds = {"started": 0, "done": 0}
        self.graph = workflow_graph(workflow)
        # redeployments for the monitor: rebind id -> {box, left, picked, done} (times)
        self.redeploys: dict[str, dict[str, Any]] = {}
        # monitor: per box, the (instance, slot)s whose request is out; when its work last ended
        self.busy: dict[str, set[tuple[int, int]]] = {n["id"]: set() for n in self.graph["nodes"]}
        self.exec_until: dict[str, float] = {}
        self.bound: set[str] = set()  # tasks whose services have been bound (setup started)
        self.open: dict[str, set[int]] = {t.id: set() for t in workflow.tasks}  # running instances
        self.cloud = {node_id(t.id, r.service_name) for t in workflow.tasks
                      for r in t.requirements if not bindable(r)}
        self.plume: dict[str, Any] | None = None
        # legacy imposes the plume in the impose_plume_service task (its plume_service task
        # only produced the KML); a workflow without one imposes it after the plume task
        self.plume_source = self.workflow.task(
            self._task_with(IMPOSE_SERVICE) or self._task_with(PLUME_SERVICE)
            or workflow.tasks[0].id)
        self.aloha = read_aloha_kml() if plume_mode == "aloha" else None
        self.vehicles = vehicles if vehicles is not None else (
            VehicleSimulator() if self._task_with(VEHICLE_SERVICE) else None)

    def _task_with(self, service: str) -> str | None:
        return next((t.id for t in self.workflow.tasks
                     if any(r.service_name == service for r in t.requirements)), None)

    # ------------------------------------------------------------------ input
    def note(self, text: str, kind: str = "info") -> None:
        self.log.appendleft({"t": round(self.t, 1), "text": text, "kind": kind})

    def on_control(self, body: dict[str, Any]) -> None:
        if body.get("type") == "start":
            self.running = True
            self.note("run started")
        elif body.get("type") == "stop":
            self.running = False
            self.note("run stopped")

    def on_message(self, msg: Message, t: float) -> None:
        self.t = t
        k, src, dst = msg.kind, msg.src, msg.dst
        if k in (MsgKind.HEARTBEAT, MsgKind.DEVICE_JOIN) and src in self.devices:
            dev = self.devices[src]
            if dev["left"] or dev["last_seen"] is None:
                self.note(f"{src} joined {dst}" if dev["last_seen"] is None else f"{src} is back")
            dev["last_seen"], dev["left"] = t, False
        elif k in (MsgKind.DATA_REQUEST, MsgKind.TASK_ASSIGN) and dst in self.devices:
            svc = str(msg.payload.get("key", "")).split("/")[2:3]
            serving = self.devices[dst]["serving"]
            if svc and svc[0] not in serving:
                serving.append(svc[0])
            box = self._box(msg)
            if box and msg.instance is not None:  # a service of this instance starts working
                self.busy[box].add((msg.instance, parse_bkey(msg.payload["key"])[1]))
        elif k == MsgKind.TASK_RESULT and src in self.devices:
            box = self._box(msg)
            if box and msg.instance is not None:  # ...and has answered
                self._idle(box, {(msg.instance, parse_bkey(msg.payload["key"])[1])})
            if box:  # a result proves the service runs again: any redeployment of it is over
                self._end_redeploys(box)
        elif (k in (MsgKind.TASK_ASSIGN, MsgKind.TASK_DELEGATE) and src == COORDINATOR_ID
              and msg.instance is None and msg.task_id in self.tasks):
            self.bound.add(msg.task_id)  # GOM: bindings sent to the peer; LOM: task delegated
        elif k == MsgKind.TASK_ASSIGN and dst != COORDINATOR_ID and msg.instance is not None:
            task = self.tasks.get(msg.task_id or "")
            if task is not None and src == COORDINATOR_ID:
                task["started"] += 1
                task["status"] = "executing"
                self.open[task["id"]].add(msg.instance)
                if task["started"] == 1 and msg.task_id == self._task_with(VEHICLE_SERVICE):
                    self._start_vehicles()
        elif k == MsgKind.TASK_RESULT and dst == COORDINATOR_ID and msg.instance is not None:
            self._end_instance(msg.task_id or "", msg.instance)
            self._task_result(msg)
        elif k == MsgKind.DEVICE_LEFT and dst == COORDINATOR_ID:
            dev_id = msg.payload.get("device_id")
            if dev_id in self.devices:
                self.devices[dev_id]["left"] = True
                self.devices[dev_id]["serving"] = []
            self.note(f"{src}: {dev_id} has left (no heartbeat)", "warn")
        elif k == MsgKind.REDEPLOY_START:
            self.rebinds["started"] += 1
            key = msg.payload.get("key")
            if key and msg.rebind_id:
                task_id, _, service, _ = parse_bkey(key)
                self._end_redeploys(node_id(task_id, service))  # superseded by this one
                self.redeploys[msg.rebind_id] = {"box": node_id(task_id, service), "left": t,
                                                 "picked": None, "done": None}
            self.note(f"{src}: redeployment {msg.rebind_id} started", "warn")
        elif k == MsgKind.REBIND_REPLY and msg.payload.get("device_id"):  # GOM
            self._picked(msg.rebind_id)
            self.note(f"coordinator picked {msg.payload['device_id']} as the replacement")
        elif k == MsgKind.DEPLOY and dst in self.devices:  # LOM: the peer picked it itself
            self._picked(msg.rebind_id)
            self.note(f"deploying {msg.payload.get('key', '').split('/')[2]} to {dst}")
        elif k == MsgKind.REDEPLOY_DONE:
            self.rebinds["done"] += 1
            rd = self.redeploys.get(msg.rebind_id or "")
            if rd is not None:
                rd["done"] = t
                rd["picked"] = t if rd["picked"] is None else rd["picked"]
            self.note(f"{src}: redeployment {msg.rebind_id} done", "ok")

    def _end_redeploys(self, box: str) -> None:
        """Close every open redeployment of ``box`` (its "done" may never come: a second
        departure can supersede it before it finishes)."""
        for rd in self.redeploys.values():
            if rd["box"] == box and rd["done"] is None:
                rd["done"] = self.t
                rd["picked"] = self.t if rd["picked"] is None else rd["picked"]

    def _picked(self, rid: str | None) -> None:
        rd = self.redeploys.get(rid or "")
        if rd is not None and rd["picked"] is None:
            rd["picked"] = self.t

    def _redeploy_state(self, box: str, t: float) -> str | None:
        """device leave until the replacement is picked, then ready for deployment until
        the redeployment is done; each shown for at least EXEC_HOLD_S."""
        inf = float("inf")
        out = None
        for rd in self.redeploys.values():
            if rd["box"] != box or t < rd["left"]:
                continue
            picked = inf if rd["picked"] is None else rd["picked"]
            red_end = max(picked, rd["left"] + EXEC_HOLD_S)
            if t < red_end:
                return "device leave"
            done = inf if rd["done"] is None else rd["done"]
            if t < max(done, red_end + EXEC_HOLD_S):
                out = "ready for deployment"
        return out

    def _box(self, msg: Message) -> str | None:
        key = msg.payload.get("key")
        if not key:
            return None
        task_id, _, service, _ = parse_bkey(key)
        box = node_id(task_id, service)
        return box if box in self.busy else None

    def _idle(self, box: str, done: set[tuple[int, int]]) -> None:
        if self.busy[box] & done:
            self.busy[box] -= done
            self.exec_until[box] = self.t + EXEC_HOLD_S

    def _end_instance(self, task_id: str, instance: int) -> None:
        """The instance is over (done or failed): none of its services is still working,
        even one whose device left without answering."""
        if task_id not in self.open:
            return
        self.open[task_id].discard(instance)
        for n in self.graph["nodes"]:
            if n["task"] == task_id:
                self._idle(n["id"], {b for b in self.busy[n["id"]] if b[0] == instance})
                if n["id"] in self.cloud:
                    self.exec_until[n["id"]] = self.t + EXEC_HOLD_S

    def _start_vehicles(self) -> None:
        if self.vehicles is not None and self.vehicles.started_at is None:
            self.vehicles.start(self.t)
            self.note("GPS simulator started: vehicles on their routes", "ok")

    def _task_result(self, msg: Message) -> None:
        task = self.tasks.get(msg.task_id or "")
        if task is None:
            return
        ok = bool(msg.payload.get("ok", True))
        task["ok" if ok else "failed"] += 1
        task["status"] = "waiting for input"
        outputs = msg.payload.get("outputs") or {}
        readings = {name: out.get("reading") for name, out in outputs.items()
                    if isinstance(out, dict) and out.get("reading")}
        if readings:
            task["readings"] = readings
        if not ok:
            self.note(f"task {task['id']} instance {msg.instance} failed", "warn")
            return
        if self.plume_mode == "computed" and PLUME_INPUTS <= set(outputs):
            spec = self.workflow.task(task["id"])
            self.plume = compute_plume(outputs, spec.location)
            p = self.plume["properties"]
            self.note(f"task {task['id']} instance {msg.instance}: plume imposed "
                      f"(wind {p['wind_m_s']} m/s from {p['wind_from_deg']}°)", "ok")
        elif (self.plume_mode == "aloha" and self.plume is None
              and task["id"] == self.plume_source.id):
            self.plume = self.aloha
            self.note(f"task {task['id']}: ALOHA plume imposed on the map", "ok")
        if task["id"] == self._task_with(REROUTE_SERVICE) and self.vehicles is not None:
            ring = threat_polygon(self.plume) if self.plume else None
            notes = self.vehicles.reroute(self.t, ring)
            for n in notes:
                self.note(n, "warn")
            if task["ok"] == 1 and not notes:
                self.note("re-route: no vehicle route enters the threat zone", "ok")

    # ------------------------------------------------------------------ output
    def monitor(self, now: float | None = None) -> list[dict[str, Any]]:
        """Legacy monitor colours per service box, for the task's current instance:

        * device leave: its device left and no replacement has been picked yet;
        * ready for deployment: a replacement was picked and is being deployed (and, at the
          start, bound but the task hasn't run yet);
        * in execution: its request is out (sensor reading / service run), or, for a cloud
          service (run on the coordinator), its task's instance is running;
        * waiting for input: between instances, waiting for the next period's data.
        """
        t = self.t if now is None else now
        out = []
        for n in self.graph["nodes"]:
            task = self.tasks[n["task"]]
            redeploy = self._redeploy_state(n["id"], t)
            if redeploy is not None:
                state = redeploy
            elif (self.busy[n["id"]] or t < self.exec_until.get(n["id"], -1.0)
                  or (n["id"] in self.cloud and self.open[n["task"]])):
                state = "in execution"
            elif n["task"] in self.bound and task["started"] == 0:
                state = "ready for deployment"
            else:
                state = "waiting for input"
            out.append({**n, "state": state})
        return out

    def snapshot(self, now: float | None = None) -> dict[str, Any]:
        t = self.t if now is None else now
        devices = []
        for d in self.devices.values():
            seen = d["last_seen"]
            away = d["left"] or seen is None or t - seen > self.away_after_s
            devices.append({**d, "state": "away" if away else
                            "serving" if d["serving"] else "idle"})
        sensors = sum(1 for t_ in self.workflow.tasks for r in t_.requirements
                      if r.kind == ServiceKind.SENSOR)
        vehicles = [{k: v for k, v in car.items() if k != "path"}
                    for car in (self.vehicles.snapshot(t) if self.vehicles else [])]
        return {"t": round(t, 1), "running": self.running,
                "tasks": list(self.tasks.values()), "devices": devices,
                "log": list(self.log), "rebinds": self.rebinds, "sensors": sensors,
                "plume": self.plume, "monitor": self.monitor(t), "vehicles": vehicles}

    def static(self) -> dict[str, Any]:
        """What doesn't change during a run (fetched once by the page)."""
        src = self.plume_source.location
        return {"workflow": self.workflow.name, "plume_mode": self.plume_mode,
                "incident": {"lat": src.lat, "lon": src.lon},
                "cells": [{"id": c.id, "lat": c.center.lat, "lon": c.center.lon,
                           "radius_m": c.radius_m} for c in self.registry.cells()],
                "edges": self.graph["edges"],
                "routes": [{"id": car["id"], "from": car["from"], "to": car["to"],
                            "path": car["path"]}
                           for car in (self.vehicles.snapshot(0.0) if self.vehicles else [])],
                "aloha": self.aloha["properties"] if self.aloha else None}
