"""Layout of the workflow monitor: one box per service, wired like legacy's monitor page.

Legacy's monitor (``MqttWeb/web-page-monitor.htm``, not in the repository; seen in the demo
video) draws the IMW as boxes left to right: the sensors of task 1 feed
computation_service_plume -> plume_service -> impose_plume_service -> vehicle_status_service
-> traffic_flow_service (which also takes camera_feed_intersection_1) -> re_route_vehicle
service. We derive the same picture from any workflow spec:

* inside a task, every sensor feeds the first non-sensor service, and the non-sensor
  services form a chain in their listed order;
* a task's last service feeds the first non-sensor service of every task that depends on it;
* columns are the longest path from the left; a sensor sits one column before the service it
  feeds (so the camera feed lines up under the vehicle status box, as in the video).
"""

from __future__ import annotations

from typing import Any

from orchflow.core.models import ServiceKind
from orchflow.core.spec import WorkflowSpec


def node_id(task_id: str, service: str) -> str:
    return f"{task_id}/{service}"


def label(service: str) -> str:
    return " ".join(w.capitalize() for w in service.split("_"))


def workflow_graph(workflow: WorkflowSpec) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[tuple[str, str]] = []
    heads: dict[str, list[str]] = {}  # task -> nodes that take the task's inputs
    tails: dict[str, list[str]] = {}  # task -> nodes whose output leaves the task
    sensors: set[str] = set()

    for task in workflow.tasks:
        sens = [node_id(task.id, r.service_name) for r in task.requirements
                if r.kind == ServiceKind.SENSOR]
        rest = [node_id(task.id, r.service_name) for r in task.requirements
                if r.kind != ServiceKind.SENSOR]
        for r in task.requirements:
            nid = node_id(task.id, r.service_name)
            nodes[nid] = {"id": nid, "task": task.id, "service": r.service_name,
                          "label": label(r.service_name), "kind": r.kind.value}
        sensors.update(sens)
        if rest:
            edges += [(s, rest[0]) for s in sens]
            edges += list(zip(rest, rest[1:], strict=False))
            heads[task.id], tails[task.id] = [rest[0]], [rest[-1]]
        else:  # a task of sensors only: its readings are its output
            heads[task.id], tails[task.id] = sens, sens
    for task in workflow.tasks:
        for dep in task.depends_on:
            edges += [(a, b) for a in tails[dep] for b in heads[task.id]]

    # longest path from the left (the graph is a DAG: tasks are validated acyclic)
    preds: dict[str, list[str]] = {n: [] for n in nodes}
    for a, b in edges:
        preds[b].append(a)
    col: dict[str, int] = {}

    def depth(n: str) -> int:
        if n not in col:
            col[n] = max((depth(p) + 1 for p in preds[n]), default=0)
        return col[n]

    for n in nodes:
        depth(n)
    for s in sensors:  # sit right before the box the sensor feeds
        succ = [b for a, b in edges if a == s]
        if succ:
            col[s] = min(col[b] for b in succ) - 1
    rows: dict[int, int] = {}
    for n in nodes:  # rows in workflow order within each column
        c = col[n]
        nodes[n]["col"], nodes[n]["row"] = c, rows.get(c, 0)
        rows[c] = rows.get(c, 0) + 1
    return {"nodes": list(nodes.values()), "edges": [list(e) for e in edges]}
