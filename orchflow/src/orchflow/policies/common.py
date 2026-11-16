"""Behaviour shared by GOM and LOM (Track 1). See docs/protocol.md for the full picture.

GOM and LOM differ in ONE thing: who picks devices and runs a redeployment (the coordinator
in GOM, the cell's peer node in LOM). Everything else is identical and lives here:

* ``DeviceLogic``     -- heartbeats to its peer, executes work when asked, replies.
* ``PeerBase``        -- runs task instances in its cell, detects departures by heartbeat
                         timeout (legacy Mobility_GOM.get_device_status_after_2_mins),
                         keeps the binding table, swaps in replacements.
* ``CoordinatorBase`` -- starts task instances on their period, records start/end.

Bindings are addressed by a string key ``"{task_id}/{slot}/{service}/{n}"``: ``slot`` is the
parallel instance (e.g. one per traffic intersection), ``n`` the n-th device for a
requirement with ``count > 1``. Keys travel in message payloads, so they must be strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from orchflow.core.events import (
    DeploymentEnded,
    DeploymentStarted,
    DeviceBound,
    RebindEnded,
    RebindStarted,
    TaskInstanceEnded,
    TaskInstanceStarted,
)
from orchflow.core.interfaces import BindingPolicy, NodeContext, Registry
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import COORDINATOR_ID, Device, PeerNode, Service, ServiceKind
from orchflow.core.plugins import create
from orchflow.core.spec import ServiceRequirement, TaskSpec, WorkflowSpec

# =========================================================================== parameters


@dataclass(frozen=True)
class Params:
    """Protocol parameters (set under ``orchestration.params`` in the experiment YAML).

    Legacy values: the peer checks every 120 s and declares a device gone when its last
    "I am Alive" is more than 120 s old (Mobility_GOM.java:63, :91). The device heartbeat
    period only exists inside the original end-device.jar; 60 s keeps one missed beat
    tolerable under the 120 s timeout.
    """

    heartbeat_s: float = 60.0
    heartbeat_timeout_s: float = 120.0
    check_interval_s: float = 120.0
    # Which bound devices the peer watches and rebinds:
    #   "deployable" = legacy (default): only devices that received a deployment, i.e. that
    #                  appear in Deployment_Task_Details (Mobility_GOM.java:86). Their other
    #                  bindings in the same task move with them (see ``carried_keys``).
    #   "all"        = every binding is watched and rebound on its own (NOT legacy).
    monitor: Literal["all", "deployable"] = "deployable"

    def __post_init__(self) -> None:
        if min(self.heartbeat_s, self.heartbeat_timeout_s, self.check_interval_s) <= 0:
            raise ValueError("heartbeat / timeout / check intervals must be > 0")
        if self.monitor not in ("all", "deployable"):
            raise ValueError(f"monitor must be 'all' or 'deployable', not {self.monitor!r}")


# =========================================================================== helpers


def bkey(task_id: str, slot: int, service: str, n: int = 0) -> str:
    return f"{task_id}/{slot}/{service}/{n}"


def parse_bkey(key: str) -> tuple[str, int, str, int]:
    task_id, slot, service, n = key.rsplit("/", 3)
    return task_id, int(slot), service, int(n)


def requirement(task: TaskSpec, service_name: str) -> ServiceRequirement:
    for req in task.requirements:
        if req.service_name == service_name:
            return req
    raise KeyError(f"task {task.id} has no requirement {service_name!r}")


def service_on(registry: Registry, device_id: str, name: str) -> Service | None:
    return next((s for s in registry.services_of(device_id) if s.name == name), None)


def task_peer(registry: Registry, task: TaskSpec) -> PeerNode:
    """The peer whose cell centre is nearest the task location (legacy
    xml_task_parser.Peer_Node_Search; the shipped code hardcoded peer 1 instead)."""
    centers = {c.id: c.center for c in registry.cells()}
    return min(registry.peers(),
               key=lambda p: (centers[p.cell_id].distance_m(task.location), p.id))


def bindable(req: ServiceRequirement) -> bool:
    """Cloud services run off-site (legacy CloudServiceHandler on the coordinator), so they
    are never bound to an end device."""
    return req.kind != ServiceKind.CLOUD


def select_device(ctx: NodeContext, policy: BindingPolicy, task: TaskSpec,
                  req: ServiceRequirement, cell_id: str, exclude: set[str]) -> Device | None:
    cands = ctx.registry.candidates(req, task.location, cell_id=cell_id,
                                    exclude=frozenset(exclude))
    return policy.select(req, cands, task.location, ctx.registry, ctx.rng)


def bind_task(ctx: NodeContext, policy: BindingPolicy, task: TaskSpec,
              cell_id: str) -> dict[str, str | None]:
    """Initial binding of every (slot, requirement, n) of ``task`` inside ``cell_id``.
    A device is never bound twice to the same service of the same task."""
    out: dict[str, str | None] = {}
    taken: dict[str, set[str]] = {}
    for slot in range(task.parallel_instances):
        for req in task.requirements:
            if not bindable(req):
                continue
            used = taken.setdefault(req.service_name, set())
            for n in range(req.count):
                dev = select_device(ctx, policy, task, req, cell_id, used)
                out[bkey(task.id, slot, req.service_name, n)] = dev.id if dev else None
                if dev is not None:
                    used.add(dev.id)
    return out


def same_service_devices(bindings: dict[str, str | None], key: str) -> set[str]:
    """Devices currently bound to the same service of the same task as ``key``."""
    task_id, _, service, _ = parse_bkey(key)
    out = set()
    for k, dev in bindings.items():
        t, _, s, _ = parse_bkey(k)
        if dev is not None and t == task_id and s == service:
            out.add(dev)
    return out


def is_monitored(params: Params, workflow: WorkflowSpec, key: str) -> bool:
    if params.monitor == "all":
        return True
    task_id, _, service, _ = parse_bkey(key)
    return requirement(workflow.task(task_id), service).deployable


def carried_keys(bindings: dict[str, str | None], key: str, failed: str, params: Params,
                 workflow: WorkflowSpec) -> list[str]:
    """Bindings that move to the replacement together with ``key``.

    Legacy rewrites the departed device's IP everywhere in the task's communication message
    (Workflow_Coordinator_Main.java:298, Mobility_LOM.java:567), so any unwatched service the
    departed device served in the same task (e.g. a sensor on the plume device) follows the
    redeployed service to the new device -- one departure, one redeployment."""
    task_id = parse_bkey(key)[0]
    return sorted(k for k, d in bindings.items()
                  if d == failed and k != key and parse_bkey(k)[0] == task_id
                  and not is_monitored(params, workflow, k))


def emit_bound(ctx: NodeContext, key: str, device_id: str, rebind_id: str | None = None) -> None:
    task_id, _, service, _ = parse_bkey(key)
    ctx.emit(DeviceBound(t=ctx.now(), node=ctx.node_id, task_id=task_id,
                         service_name=service, device_id=device_id, rebind_id=rebind_id))


def start_deploy(ctx: NodeContext, key: str, device_id: str, *, rebind_id: str | None,
                 extra: dict[str, Any] | None = None) -> bool:
    """Start transferring the artifact of ``key``'s service to ``device_id``. Returns False
    (nothing started) if the service has no artifact. Legacy: SFTP of jar + config files
    by the node that chose the device (coordinator in GOM, peer in LOM)."""
    _, _, service, _ = parse_bkey(key)
    svc = service_on(ctx.registry, device_id, service)
    if svc is None or not svc.deployable or svc.artifact is None:
        return False
    ctx.emit(DeploymentStarted(t=ctx.now(), node=ctx.node_id, device_id=device_id,
                               artifact=svc.artifact.name, rebind_id=rebind_id))
    data = {"device_id": device_id, "key": key, "rebind_id": rebind_id,
            "artifact": svc.artifact.name, **(extra or {})}
    ctx.start_work(f"deploy:{rebind_id or 'setup'}:{key}", "deploy", service=svc,
                   artifact=svc.artifact, data=data)
    return True


def emit_deploy_ended(ctx: NodeContext, ok: bool, data: dict[str, Any]) -> None:
    ctx.emit(DeploymentEnded(t=ctx.now(), node=ctx.node_id, device_id=data["device_id"],
                             artifact=data["artifact"], ok=ok, rebind_id=data["rebind_id"]))


class _Quiet:
    def on_start(self, ctx: NodeContext) -> None: ...
    def on_message(self, ctx: NodeContext, msg: Message) -> None: ...
    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None: ...
    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None: ...
    def on_depart(self, ctx: NodeContext, graceful: bool) -> None: ...


# =========================================================================== device


class DeviceLogic(_Quiet):
    """An end device. Same in GOM and LOM (legacy end-device/Main_Rasp_IoT.java)."""

    def __init__(self, device: Device, params: Params) -> None:
        self.device = device
        self.params = params

    def on_start(self, ctx: NodeContext) -> None:
        ctx.send(Message(kind=MsgKind.DEVICE_JOIN, src=ctx.node_id, dst=self.device.peer_id))
        ctx.set_timer("heartbeat", self.params.heartbeat_s)

    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None:
        if name == "heartbeat":  # legacy "I am Alive"
            ctx.send(Message(kind=MsgKind.HEARTBEAT, src=ctx.node_id, dst=self.device.peer_id))
            ctx.set_timer("heartbeat", self.params.heartbeat_s)

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind in (MsgKind.DATA_REQUEST, MsgKind.TASK_ASSIGN) and msg.instance is not None:
            self._execute(ctx, msg, reply_to=msg.src)
        elif msg.kind == MsgKind.START_SERVICE:  # legacy "Resume Execution"
            self._execute(ctx, msg, reply_to=self.device.peer_id)
        # DEPLOY / TASK_ASSIGN without instance / DEVICE_LEFT: nothing to do -- the artifact
        # is transferred by the sender's deploy work, and the wiring is the peer's business.

    def _execute(self, ctx: NodeContext, msg: Message, reply_to: str) -> None:
        key = msg.payload["key"]
        svc = service_on(ctx.registry, self.device.id, parse_bkey(key)[2])
        ctx.start_work(f"exec:{msg.id}", "execute", service=svc,
                       data={"reply_to": reply_to, "task_id": msg.task_id,
                             "instance": msg.instance, "key": key,
                             "rebind_id": msg.rebind_id})

    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None:
        ctx.send(Message(kind=MsgKind.TASK_RESULT, src=ctx.node_id, dst=data["reply_to"],
                         task_id=data["task_id"], instance=data["instance"],
                         rebind_id=data["rebind_id"],
                         # live runtime: the stub's output (services/), like legacy "<response>"
                         payload={"key": data["key"], "ok": ok, "output": data.get("output")}))

    def on_depart(self, ctx: NodeContext, graceful: bool) -> None:
        # Legacy devices never announce departure; the peer notices the missing heartbeats.
        ctx.cancel_timer("heartbeat")


# =========================================================================== peer


class PeerBase(_Quiet):
    """Peer-node behaviour common to GOM and LOM. Subclasses implement ``start_rebind``."""

    def __init__(self, peer: PeerNode, params: Params) -> None:
        self.peer = peer
        self.params = params
        self.bindings: dict[str, str | None] = {}
        # (task_id, instance) -> {binding key: device id we are still waiting for}
        self.pending: dict[tuple[str, int], dict[str, str]] = {}
        self.last_seen: dict[str, float] = {}
        self.lost: set[str] = set()
        self.rebinds: dict[str, str] = {}  # rebind_id -> binding key
        self.rebind_failed: dict[str, str] = {}  # binding key -> device that left
        self._rb_counter = 0
        self._policy: BindingPolicy | None = None

    def policy(self, ctx: NodeContext) -> BindingPolicy:
        policy = self._policy
        if policy is None:
            policy = self._policy = create("binding", ctx.experiment.binding)
        return policy

    def new_rebind_id(self) -> str:
        self._rb_counter += 1
        return f"rb-{self.peer.id}-{self._rb_counter}"

    # ------------------------------------------------------------------ lifecycle
    def on_start(self, ctx: NodeContext) -> None:
        for d in ctx.registry.devices():
            if d.peer_id == self.peer.id:
                self.last_seen[d.id] = ctx.now()
        ctx.set_timer("check", self.params.check_interval_s)

    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None:
        if name == "check":
            self.check_devices(ctx)
            ctx.set_timer("check", self.params.check_interval_s)

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind in (MsgKind.HEARTBEAT, MsgKind.DEVICE_JOIN):
            self.seen(ctx, msg.src)
        elif msg.kind == MsgKind.TASK_ASSIGN and msg.instance is not None:
            self.run_instance(ctx, msg.task_id or "", msg.instance, int(msg.payload["slot"]))
        elif msg.kind == MsgKind.TASK_RESULT and msg.src != COORDINATOR_ID:
            self.on_device_result(ctx, msg)

    # ------------------------------------------------------------------ liveness
    def seen(self, ctx: NodeContext, device_id: str) -> None:
        self.last_seen[device_id] = ctx.now()
        if device_id in self.lost:
            self.lost.discard(device_id)
            ctx.registry.set_available(device_id, True)

    def monitored(self, ctx: NodeContext, key: str) -> bool:
        return is_monitored(self.params, ctx.workflow, key)

    def check_devices(self, ctx: NodeContext) -> None:
        """Legacy Mobility_GOM.get_device_status_after_2_mins: a bound device whose last
        heartbeat is older than the timeout has left."""
        now = ctx.now()
        watched = sorted({d for k, d in self.bindings.items()
                          if d is not None and self.monitored(ctx, k)})
        for dev in watched:
            if dev in self.lost:
                continue
            if now - self.last_seen.get(dev, now) > self.params.heartbeat_timeout_s:
                self.lost.add(dev)
                ctx.registry.set_available(dev, False)
                for key in sorted(k for k, d in self.bindings.items()
                                  if d == dev and self.monitored(ctx, k)):
                    self.rebind_failed[key] = dev
                    self.start_rebind(ctx, key, dev)

    def start_rebind(self, ctx: NodeContext, key: str, failed: str) -> None:
        raise NotImplementedError

    def emit_rebind_started(self, ctx: NodeContext, rid: str, key: str, failed: str) -> None:
        task_id, _, service, _ = parse_bkey(key)
        ctx.emit(RebindStarted(t=ctx.now(), node=ctx.node_id, rebind_id=rid, task_id=task_id,
                               service_name=service, failed_device_id=failed))

    def emit_rebind_ended(self, ctx: NodeContext, rid: str, new: str | None) -> None:
        ctx.emit(RebindEnded(t=ctx.now(), node=ctx.node_id, rebind_id=rid,
                             new_device_id=new, ok=new is not None))

    # ------------------------------------------------------------------ bindings
    def take_over(self, ctx: NodeContext, key: str, new: str) -> None:
        """``new`` replaces the departed device on ``key`` and on everything it carries."""
        failed = self.rebind_failed.pop(key, None)
        moved = (carried_keys(self.bindings, key, failed, self.params, ctx.workflow)
                 if failed is not None else [])
        for k in [key, *moved]:
            self.replace(ctx, k, new)

    def replace(self, ctx: NodeContext, key: str, new: str) -> None:
        old = self.bindings.get(key)
        self.bindings[key] = new
        self.last_seen[new] = ctx.now()
        for waiting in self.pending.values():
            if old is not None and waiting.get(key) == old:
                waiting[key] = new

    def fail_key(self, ctx: NodeContext, key: str) -> None:
        """No replacement could be found: every instance waiting on ``key`` (or on what it
        carries) fails."""
        failed = self.rebind_failed.pop(key, None)
        moved = (carried_keys(self.bindings, key, failed, self.params, ctx.workflow)
                 if failed is not None else [])
        for k in moved:
            self.bindings[k] = None
            self.fail_key(ctx, k)
        self.bindings[key] = None
        for (task_id, inst), waiting in sorted(self.pending.items()):
            if key in waiting:
                del self.pending[(task_id, inst)]
                self.report(ctx, task_id, inst, ok=False)

    # ------------------------------------------------------------------ task instances
    def run_instance(self, ctx: NodeContext, task_id: str, instance: int, slot: int) -> None:
        task = ctx.workflow.task(task_id)
        waiting: dict[str, str] = {}
        for key in sorted(k for k in self.bindings if parse_bkey(k)[:2] == (task_id, slot)):
            dev = self.bindings[key]
            if dev is None:  # nothing could be bound for this requirement
                self.report(ctx, task_id, instance, ok=False)
                return
            waiting[key] = dev
        for key, dev in waiting.items():
            if dev in self.lost:
                continue  # rebind in progress: the replacement's first result will serve it
            req = requirement(task, parse_bkey(key)[2])
            kind = MsgKind.DATA_REQUEST if req.kind == ServiceKind.SENSOR else MsgKind.TASK_ASSIGN
            ctx.send(Message(kind=kind, src=ctx.node_id, dst=dev, task_id=task_id,
                             instance=instance, payload={"key": key}))
        if waiting:
            self.pending[(task_id, instance)] = waiting
        else:
            self.report(ctx, task_id, instance, ok=True)

    def on_device_result(self, ctx: NodeContext, msg: Message) -> None:
        key, dev, ok = msg.payload["key"], msg.src, bool(msg.payload.get("ok", True))
        if msg.rebind_id in self.rebinds and self.bindings.get(key) != dev:
            self.take_over(ctx, key, dev)
        inst = msg.instance
        resumed = inst is None  # reply to START_SERVICE ("Resume Execution")
        targets = sorted(self.pending) if inst is None else [(msg.task_id or "", inst)]
        for tk in targets:
            waiting = self.pending.get(tk)
            if waiting is None:
                continue
            # a resumed device serves every slot it took over in the redeployment, including
            # carried sensors; a normal reply serves only the slot it was asked for
            served = ([k for k, d in waiting.items() if d == dev and parse_bkey(k)[0] == tk[0]]
                      if resumed else [key] if waiting.get(key) == dev else [])
            if not served:
                continue
            if not ok:
                del self.pending[tk]
                self.report(ctx, tk[0], tk[1], ok=False)
                continue
            for k in served:
                del waiting[k]
            if not waiting:
                del self.pending[tk]
                self.report(ctx, tk[0], tk[1], ok=True)
        if msg.rebind_id in self.rebinds:
            self.on_replacement_running(ctx, msg.rebind_id, dev)

    def on_replacement_running(self, ctx: NodeContext, rid: str, dev: str) -> None:
        """The replacement device of rebind ``rid`` produced its first result."""
        self.rebinds.pop(rid, None)

    def report(self, ctx: NodeContext, task_id: str, instance: int, ok: bool) -> None:
        ctx.send(Message(kind=MsgKind.TASK_RESULT, src=ctx.node_id, dst=COORDINATOR_ID,
                         task_id=task_id, instance=instance, payload={"ok": ok}))


# =========================================================================== coordinator


class CoordinatorBase(_Quiet):
    """Starts task instances on their period once the task is set up and its dependencies
    have completed at least one instance (legacy Workflow_Coordinator_Main task loop)."""

    def __init__(self) -> None:
        self.task_peers: dict[str, str] = {}
        self.ready: set[str] = set()
        self.started: set[str] = set()
        self.completed: set[str] = set()
        self.instances: dict[str, int] = {}
        self._policy: BindingPolicy | None = None

    def policy(self, ctx: NodeContext) -> BindingPolicy:
        policy = self._policy
        if policy is None:
            policy = self._policy = create("binding", ctx.experiment.binding)
        return policy

    def mark_ready(self, ctx: NodeContext, task_id: str) -> None:
        self.ready.add(task_id)
        self.maybe_start(ctx)

    def maybe_start(self, ctx: NodeContext) -> None:
        for task in ctx.workflow.topological_order():
            if (task.id in self.ready and task.id not in self.started
                    and all(d in self.completed for d in task.depends_on)):
                self.started.add(task.id)
                ctx.set_timer(f"tick:{task.id}", 0.0, {"task_id": task.id})

    def on_timer(self, ctx: NodeContext, name: str, data: dict[str, Any]) -> None:
        if not name.startswith("tick:"):
            return
        task = ctx.workflow.task(data["task_id"])
        for slot in range(task.parallel_instances):
            n = self.instances[task.id] = self.instances.get(task.id, 0) + 1
            deadline = ctx.now() + task.deadline_s if task.deadline_s is not None else None
            ctx.emit(TaskInstanceStarted(t=ctx.now(), node=ctx.node_id, task_id=task.id,
                                         instance=n, deadline_at=deadline))
            ctx.send(Message(kind=MsgKind.TASK_ASSIGN, src=ctx.node_id,
                             dst=self.task_peers[task.id], task_id=task.id, instance=n,
                             payload={"slot": slot}))
        if task.period_s is not None:
            ctx.set_timer(name, task.period_s, data)

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.TASK_RESULT and msg.instance is not None:
            task = ctx.workflow.task(msg.task_id or "")
            ok = bool(msg.payload.get("ok", False))
            if ok and any(not bindable(r) for r in task.requirements):
                # cloud part of the task runs here (legacy CloudServiceHandler)
                ctx.start_work(f"cloud:{task.id}:{msg.instance}", "execute",
                               data={"task_id": task.id, "instance": msg.instance})
            else:
                self.end_instance(ctx, task.id, msg.instance, ok)

    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None:
        if work_id.startswith("cloud:"):
            self.end_instance(ctx, data["task_id"], data["instance"], ok)

    def end_instance(self, ctx: NodeContext, task_id: str, instance: int, ok: bool) -> None:
        ctx.emit(TaskInstanceEnded(t=ctx.now(), node=ctx.node_id, task_id=task_id,
                                   instance=instance, ok=ok))
        if ok and task_id not in self.completed:
            self.completed.add(task_id)
            self.maybe_start(ctx)
