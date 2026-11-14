"""``orchestration: {name: gom}`` -- Global Orchestration Mode (Track 1).

The coordinator picks every device and runs every redeployment; peers only run task
instances and watch their devices. Port of legacy GOM_Approach.java,
Workflow_Coordinator_Main.java (GOM branch) and peer-node Mobility_GOM.java.

Redeployment (docs/protocol.md has the full table with legacy file:line references):

    peer  -> coord   REDEPLOY_START     "<hint>[k]</hint> Start"
    peer  -> coord   DEVICE_LEFT        "IP Address: [ip] Device has left {k}"
                     (coordinator picks a replacement; deploys the artifact if needed)
    coord -> peer    REBIND_REPLY       "New IP_Address = ..."
    coord -> new     DEPLOY             deployment message
    coord -> new     TASK_ASSIGN        communication message
    coord -> new     START_SERVICE      "Resume Execution"
    new   -> peer    TASK_RESULT        first execution response
    peer  -> coord   REDEPLOY_DONE      "<hint>[k]</hint> End"

All 8 carry the rebind_id, so they are the redeployment's message overhead.
"""

from __future__ import annotations

from typing import Any

from orchflow.core.interfaces import NodeContext, NodeLogic
from orchflow.core.messages import Message, MsgKind
from orchflow.core.models import COORDINATOR_ID, Device, PeerNode
from orchflow.core.plugins import register
from orchflow.policies.common import (
    CoordinatorBase,
    DeviceLogic,
    Params,
    PeerBase,
    bind_task,
    carried_keys,
    emit_bound,
    emit_deploy_ended,
    parse_bkey,
    requirement,
    same_service_devices,
    select_device,
    start_deploy,
    task_peer,
)


class GomCoordinator(CoordinatorBase):
    def __init__(self, params: Params) -> None:
        super().__init__()
        self.params = params
        self.bindings: dict[str, str | None] = {}
        self.task_cells: dict[str, str] = {}
        self.setup_left: dict[str, int] = {}  # task -> deployments still running

    def on_start(self, ctx: NodeContext) -> None:
        for task in ctx.workflow.topological_order():
            peer = task_peer(ctx.registry, task)
            self.task_peers[task.id], self.task_cells[task.id] = peer.id, peer.cell_id
            bound = bind_task(ctx, self.policy(ctx), task, peer.cell_id)
            self.bindings.update(bound)
            for key, dev in sorted(bound.items()):
                if dev is not None:
                    emit_bound(ctx, key, dev)
            # communication message: tell the peer which device serves what
            ctx.send(Message(kind=MsgKind.TASK_ASSIGN, src=ctx.node_id, dst=peer.id,
                             task_id=task.id, payload={"bindings": bound}))
            started = sum(start_deploy(ctx, k, d, rebind_id=None, extra={"task_id": task.id})
                          for k, d in sorted(bound.items()) if d is not None)
            self.setup_left[task.id] = started
            if started == 0:
                self.mark_ready(ctx, task.id)

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.DEVICE_LEFT and msg.rebind_id is not None:
            self.redeploy(ctx, msg)
        else:  # REDEPLOY_START / REDEPLOY_DONE are notifications only (logged by runtime)
            super().on_message(ctx, msg)

    def redeploy(self, ctx: NodeContext, msg: Message) -> None:
        """Legacy GOM_Approach.selectAlternateDevice + Workflow_Coordinator_Main:259-350.
        Unlike legacy, the departed device is excluded (legacy could re-pick it)."""
        rid, key, failed = msg.rebind_id or "", msg.payload["key"], msg.payload["device_id"]
        task_id, _, service, _ = parse_bkey(key)
        task = ctx.workflow.task(task_id)
        exclude = same_service_devices(self.bindings, key) | {failed}
        new = select_device(ctx, self.policy(ctx), task, requirement(task, service),
                            self.task_cells[task_id], exclude)
        if new is None:
            self.bindings[key] = None
            ctx.send(Message(kind=MsgKind.REBIND_REPLY, src=ctx.node_id, dst=msg.src,
                             task_id=task_id, rebind_id=rid,
                             payload={"key": key, "device_id": None}))
            return
        for k in [key, *carried_keys(self.bindings, key, failed, self.params, ctx.workflow)]:
            self.bindings[k] = new.id
        emit_bound(ctx, key, new.id, rid)
        if not start_deploy(ctx, key, new.id, rebind_id=rid,
                            extra={"task_id": task_id, "peer": msg.src}):
            self.send_rebind(ctx, rid, key, new.id, msg.src)

    def send_rebind(self, ctx: NodeContext, rid: str, key: str, new: str, peer: str) -> None:
        """The four messages of Workflow_Coordinator_Main.java:341-350, in legacy order."""
        task_id = parse_bkey(key)[0]
        common: dict[str, Any] = {"src": ctx.node_id, "task_id": task_id, "rebind_id": rid}
        ctx.send(Message(kind=MsgKind.REBIND_REPLY, dst=peer,
                         payload={"key": key, "device_id": new}, **common))
        ctx.send(Message(kind=MsgKind.DEPLOY, dst=new, payload={"key": key}, **common))
        ctx.send(Message(kind=MsgKind.TASK_ASSIGN, dst=new, payload={"key": key}, **common))
        ctx.send(Message(kind=MsgKind.START_SERVICE, dst=new, payload={"key": key}, **common))

    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None:
        if not work_id.startswith("deploy:"):
            super().on_work_done(ctx, work_id, ok, data)
            return
        emit_deploy_ended(ctx, ok, data)
        rid = data["rebind_id"]
        if rid is None:  # initial deployment
            self.setup_left[data["task_id"]] -= 1
            if self.setup_left[data["task_id"]] == 0:
                self.mark_ready(ctx, data["task_id"])
        else:
            # legacy sends the four messages even if the upload failed; so do we
            self.send_rebind(ctx, rid, data["key"], data["device_id"], data["peer"])


class GomPeer(PeerBase):
    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.TASK_ASSIGN and "bindings" in msg.payload:
            for key, dev in msg.payload["bindings"].items():
                self.bindings[key] = dev
                if dev is not None:
                    self.last_seen.setdefault(dev, ctx.now())
        elif msg.kind == MsgKind.REBIND_REPLY and msg.rebind_id in self.rebinds:
            key, new = msg.payload["key"], msg.payload["device_id"]
            if new is None:
                self.rebinds.pop(msg.rebind_id or "")
                self.emit_rebind_ended(ctx, msg.rebind_id or "", None)
                self.fail_key(ctx, key)
            elif self.bindings.get(key) != new:
                self.take_over(ctx, key, new)
        else:
            super().on_message(ctx, msg)

    def start_rebind(self, ctx: NodeContext, key: str, failed: str) -> None:
        """Legacy Mobility_GOM.java:97-115 (hint Start) and :239 (Device has left)."""
        rid = self.new_rebind_id()
        self.rebinds[rid] = key
        self.emit_rebind_started(ctx, rid, key, failed)
        task_id = parse_bkey(key)[0]
        for kind, payload in ((MsgKind.REDEPLOY_START, {"key": key}),
                              (MsgKind.DEVICE_LEFT, {"key": key, "device_id": failed})):
            ctx.send(Message(kind=kind, src=ctx.node_id, dst=COORDINATOR_ID, task_id=task_id,
                             rebind_id=rid, payload=payload))

    def on_replacement_running(self, ctx: NodeContext, rid: str, dev: str) -> None:
        """Legacy Main_Rasp_Combine.java:451-481: first response after a departure ->
        "<hint>[k]</hint> End" to the coordinator."""
        key = self.rebinds.pop(rid)
        ctx.send(Message(kind=MsgKind.REDEPLOY_DONE, src=ctx.node_id, dst=COORDINATOR_ID,
                         task_id=parse_bkey(key)[0], rebind_id=rid, payload={"key": key}))
        self.emit_rebind_ended(ctx, rid, dev)


@register("orchestration", "gom")
class GomProtocol:
    name = "gom"

    def __init__(self, **params: Any) -> None:
        self.params = Params(**params)

    def coordinator(self) -> NodeLogic:
        return GomCoordinator(self.params)

    def peer(self, peer: PeerNode) -> NodeLogic:
        return GomPeer(peer, self.params)

    def device(self, device: Device) -> NodeLogic:
        return DeviceLogic(device, self.params)
