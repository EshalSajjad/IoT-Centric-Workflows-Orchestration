"""``orchestration: {name: lom}`` -- Local Orchestration Mode (Track 1).

The coordinator delegates each task to the peer of the task's cell; that peer picks the
devices and runs redeployments itself, using only its own cell. Port of legacy
LOM_Approach.java and peer-node Mobility_LOM.java / xml_task_parser.java.

Redeployment (docs/protocol.md has the full table with legacy file:line references):

    peer -> failed   DEVICE_LEFT        "IP Address [ip] Device has left"  (dropped: it's gone)
    peer -> coord    REDEPLOY_START     "<Task>[k]</Task> Device left"
    peer -> coord    DEVICE_LEFT        "IP Address [ip] Device has left"
                     (peer picks a replacement in its own cell; deploys the artifact)
    peer -> coord    REDEPLOY_DONE      "Redeployment Done [k]"
    peer -> new      START_SERVICE      "Resume Execution" (*)
    new  -> peer     TASK_RESULT        first execution response

(*) commented out in the shipped source (Mobility_LOM.java:800-890), which left the new
device with no way to start; we keep only this one message of that block.

Not ported (hardcoded to the authors' Raspberry Pi testbed): ``Selected_Peer_Node = 1``
and the fixed ``10.103.72.x`` address rewrites -- replaced by the registry.
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
    emit_bound,
    emit_deploy_ended,
    parse_bkey,
    requirement,
    same_service_devices,
    select_device,
    start_deploy,
    task_peer,
)


class LomCoordinator(CoordinatorBase):
    def on_start(self, ctx: NodeContext) -> None:
        for task in ctx.workflow.topological_order():
            peer = task_peer(ctx.registry, task)
            self.task_peers[task.id] = peer.id
            ctx.send(Message(kind=MsgKind.TASK_DELEGATE, src=ctx.node_id, dst=peer.id,
                             task_id=task.id))

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.BIND_ACK and msg.task_id is not None:
            self.mark_ready(ctx, msg.task_id)
        else:
            super().on_message(ctx, msg)


class LomPeer(PeerBase):
    def __init__(self, peer: PeerNode, params: Any) -> None:
        super().__init__(peer, params)
        self.setup_left: dict[str, int] = {}

    def on_message(self, ctx: NodeContext, msg: Message) -> None:
        if msg.kind == MsgKind.TASK_DELEGATE and msg.task_id is not None:
            self.setup_task(ctx, msg.task_id)
        else:
            super().on_message(ctx, msg)

    def setup_task(self, ctx: NodeContext, task_id: str) -> None:
        """Legacy LOM: the peer parses the delegated workflow and searches its own registry
        (xml_task_parser.Database_Search / Device_Search)."""
        task = ctx.workflow.task(task_id)
        bound = bind_task(ctx, self.policy(ctx), task, self.peer.cell_id)
        self.bindings.update(bound)
        for key, dev in sorted(bound.items()):
            if dev is not None:
                emit_bound(ctx, key, dev)
                self.last_seen.setdefault(dev, ctx.now())
        started = sum(start_deploy(ctx, k, d, rebind_id=None, extra={"task_id": task_id})
                      for k, d in sorted(bound.items()) if d is not None)
        self.setup_left[task_id] = started
        if started == 0:
            self.ack(ctx, task_id)

    def ack(self, ctx: NodeContext, task_id: str) -> None:
        ctx.send(Message(kind=MsgKind.BIND_ACK, src=ctx.node_id, dst=COORDINATOR_ID,
                         task_id=task_id))

    def start_rebind(self, ctx: NodeContext, key: str, failed: str) -> None:
        """Legacy Mobility_LOM.get_device_status_after_2_mins (:348-403) then the
        Alternate_Device_Search block (:541-796)."""
        rid = self.new_rebind_id()
        self.rebinds[rid] = key
        self.emit_rebind_started(ctx, rid, key, failed)
        task_id, _, service, _ = parse_bkey(key)
        common: dict[str, Any] = {"src": ctx.node_id, "task_id": task_id, "rebind_id": rid}
        ctx.send(Message(kind=MsgKind.DEVICE_LEFT, dst=failed,
                         payload={"key": key, "device_id": failed}, **common))
        ctx.send(Message(kind=MsgKind.REDEPLOY_START, dst=COORDINATOR_ID,
                         payload={"key": key}, **common))
        ctx.send(Message(kind=MsgKind.DEVICE_LEFT, dst=COORDINATOR_ID,
                         payload={"key": key, "device_id": failed}, **common))

        task = ctx.workflow.task(task_id)
        exclude = same_service_devices(self.bindings, key) | {failed}
        new = select_device(ctx, self.policy(ctx), task, requirement(task, service),
                            self.peer.cell_id, exclude)
        if new is None:
            self.rebinds.pop(rid)
            self.emit_rebind_ended(ctx, rid, None)
            self.fail_key(ctx, key)
            return
        self.take_over(ctx, key, new.id)
        emit_bound(ctx, key, new.id, rid)
        if not start_deploy(ctx, key, new.id, rebind_id=rid, extra={"task_id": task_id}):
            self.finish_rebind(ctx, rid, key, new.id)

    def finish_rebind(self, ctx: NodeContext, rid: str, key: str, new: str) -> None:
        task_id = parse_bkey(key)[0]
        ctx.send(Message(kind=MsgKind.REDEPLOY_DONE, src=ctx.node_id, dst=COORDINATOR_ID,
                         task_id=task_id, rebind_id=rid, payload={"key": key}))
        self.emit_rebind_ended(ctx, rid, new)
        ctx.send(Message(kind=MsgKind.START_SERVICE, src=ctx.node_id, dst=new,
                         task_id=task_id, rebind_id=rid, payload={"key": key}))

    def on_work_done(self, ctx: NodeContext, work_id: str, ok: bool,
                     data: dict[str, Any]) -> None:
        if not work_id.startswith("deploy:"):
            return
        emit_deploy_ended(ctx, ok, data)
        rid, key = data["rebind_id"], data["key"]
        if rid is None:
            self.setup_left[data["task_id"]] -= 1
            if self.setup_left[data["task_id"]] == 0:
                self.ack(ctx, data["task_id"])
        elif rid in self.rebinds:
            # legacy reports "Redeployment Done" after the upload whatever its outcome
            self.finish_rebind(ctx, rid, key, data["device_id"])


@register("orchestration", "lom")
class LomProtocol:
    name = "lom"

    def __init__(self, **params: Any) -> None:
        # legacy LOM peer loop runs every 30 s (Mobility_LOM.java:93); GOM's every 120 s
        self.params = Params(**{"check_interval_s": 30.0, **params})

    def coordinator(self) -> NodeLogic:
        return LomCoordinator()

    def peer(self, peer: PeerNode) -> NodeLogic:
        return LomPeer(peer, self.params)

    def device(self, device: Device) -> NodeLogic:
        return DeviceLogic(device, self.params)
