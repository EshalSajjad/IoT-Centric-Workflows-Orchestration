"""LiveAgent: one OS process that hosts one or many nodes (coordinator, peers, devices).

    orchflow agent experiment.yaml --nodes coord     # just the coordinator
    orchflow agent experiment.yaml --nodes peers     # every peer node
    orchflow agent experiment.yaml --nodes devices   # every device
    orchflow agent experiment.yaml --nodes all       # everything in one process

What an agent does, in order:
 1. builds the registry from the testbed YAML (identical in every process, same seed)
 2. builds the logic object for each node it hosts from the orchestration plug-in
    (echo now; gom / lom once Track 1 lands; no code change needed here)
 3. subscribes to each hosted node's inbox, orchflow/{node_id}/in, plus orchflow/_control
 4. start barrier: the agent hosting "coord" is the LEADER. Others announce "ready" every
    second; when every node in the testbed is ready (or ready_timeout_s passes) the leader
    broadcasts "start". Without this, the coordinator's first messages would reach peers
    that are not listening yet and be lost.
 5. on start: clock = 0, on_start() for every node, devices begin leaving / rejoining
    following the mobility plug-in (if one is registered)
 6. inbox message -> Message.from_json -> logic.on_message(ctx, msg)
 7. after duration_s the leader broadcasts "stop"; everyone shuts down and closes its log

Device departures in one process must be visible to the others (each has its own copy of
the registry), so the agent broadcasts {"type": "availability"} on the control topic. This
is housekeeping, not protocol traffic: it is not a Message and is never counted.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from orchflow.core import plugins
from orchflow.core.events import DeviceJoined, DeviceLeft, RunEnded, RunStarted
from orchflow.core.interfaces import MobilityModel, NodeLogic, Registry
from orchflow.core.messages import Message
from orchflow.core.models import COORDINATOR_ID, Device
from orchflow.core.spec import ExperimentSpec, TestbedSpec, WorkflowSpec, load_yaml
from orchflow.runtime.live.context import LiveContext
from orchflow.runtime.live.logs import LineEventSink
from orchflow.runtime.live.transport import (
    CONTROL_TOPIC,
    Transport,
    inbox_topic,
    node_of_topic,
)
from orchflow.runtime.live.work import SleepWork, WorkRunner

log = logging.getLogger("orchflow.live")
msg_log = logging.getLogger("orchflow.live.msg")  # one line per message sent


# =========================================================================== inputs


def load_inputs(experiment_path: str | Path, *, duration_s: float | None = None,
                ) -> tuple[ExperimentSpec, WorkflowSpec, TestbedSpec]:
    exp = load_yaml(experiment_path, ExperimentSpec)
    if duration_s is not None:
        exp = exp.model_copy(update={"duration_s": duration_s})
    return exp, load_yaml(exp.workflow, WorkflowSpec), load_yaml(exp.testbed, TestbedSpec)


def build_registry(testbed: TestbedSpec) -> Registry:
    """Track 1's core/registry.build_registry once it exists; until then the tiny fake
    registry with the testbed's shape (n_peers x devices_per_peer)."""
    try:
        from orchflow.core.registry import build_registry as real  # type: ignore
    except ModuleNotFoundError as e:
        if e.name != "orchflow.core.registry":
            raise
        from orchflow.core.testing import tiny_registry

        log.warning("core/registry.py not written yet (Track 1): using tiny_registry(%d, %d)",
                    testbed.n_peers, testbed.devices_per_peer)
        return tiny_registry(testbed.n_peers, testbed.devices_per_peer)
    return real(testbed)


def all_nodes(registry: Registry) -> list[str]:
    return ([COORDINATOR_ID] + [p.id for p in registry.peers()]
            + [d.id for d in registry.devices()])


def select_nodes(spec: str, registry: Registry) -> list[str]:
    """'all' | 'coord' | 'peers' | 'devices' | 'devices@peer-01' | node ids, comma-separated."""
    known = all_nodes(registry)
    out: list[str] = []
    for tok in (t.strip() for t in spec.split(",") if t.strip()):
        if tok == "all":
            out += known
        elif tok in ("coord", COORDINATOR_ID):
            out.append(COORDINATOR_ID)
        elif tok == "peers":
            out += [p.id for p in registry.peers()]
        elif tok == "devices":
            out += [d.id for d in registry.devices()]
        elif tok.startswith("devices@"):
            peer = tok.split("@", 1)[1]
            registry.peer(peer)  # KeyError if unknown
            out += [d.id for d in registry.devices() if d.peer_id == peer]
        elif tok in known:
            out.append(tok)
        else:
            raise ValueError(f"unknown node {tok!r} (not in the testbed)")
    return list(dict.fromkeys(out))  # dedupe, keep order


def load_mobility(experiment: ExperimentSpec) -> MobilityModel | None:
    try:
        return plugins.create("mobility", experiment.mobility)
    except KeyError:
        log.warning("no mobility plug-in %r registered yet (Track 2): devices never leave",
                    experiment.mobility.name)
        return None


# =========================================================================== agent


class LiveAgent:
    def __init__(
        self,
        *,
        experiment: ExperimentSpec,
        workflow: WorkflowSpec,
        registry: Registry,
        nodes: Sequence[str],
        transport: Transport,
        run_dir: str | Path,
        seed: int = 0,
        name: str = "agent",
        work: WorkRunner | None = None,
        mobility: MobilityModel | None = None,
        ready_timeout_s: float = 60.0,
    ) -> None:
        self.experiment, self.workflow, self.registry = experiment, workflow, registry
        self.transport, self.seed, self.name = transport, seed, name
        self.work: WorkRunner = work or SleepWork()
        self.mobility = mobility
        self.ready_timeout_s = ready_timeout_s
        self.run_dir = Path(run_dir)
        self.sink = LineEventSink(self.run_dir / f"events-{name}.jsonl")

        self.leader = COORDINATOR_ID in nodes
        self.expected = set(all_nodes(registry))
        self.run_id: str | None = None
        self.logic_errors = 0

        protocol = plugins.create("orchestration", experiment.orchestration)
        self.contexts: dict[str, LiveContext] = {}
        for nid in nodes:
            self.contexts[nid] = LiveContext(nid, self._make_logic(protocol, nid), self, seed)

        self._t0: float | None = None
        self._outbox: asyncio.Queue[Message] = asyncio.Queue()
        self._pending: list[Message] = []  # inbox messages that arrived before "start"
        self._ready: set[str] = set()
        self._all_ready = asyncio.Event()
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    def _make_logic(self, protocol: Any, nid: str) -> NodeLogic:
        if nid == COORDINATOR_ID:
            return protocol.coordinator()
        try:
            return protocol.peer(self.registry.peer(nid))
        except KeyError:
            return protocol.device(self.registry.device(nid))

    # ------------------------------------------------------------------ used by LiveContext
    def now(self) -> float:
        if self._t0 is None:
            return 0.0
        return asyncio.get_running_loop().time() - self._t0

    def enqueue(self, msg: Message) -> None:
        rb = f"  rebind={msg.rebind_id}" if msg.rebind_id else ""
        msg_log.info("%8.2fs  %-9s -> %-9s %s%s", msg.sent_at or 0.0, msg.src, msg.dst,
                     msg.kind.value, rb)
        self._outbox.put_nowait(msg)

    def call_logic(self, nid: str, fn: Callable[..., None], *args: Any) -> None:
        try:
            fn(*args)
        except Exception:
            self.logic_errors += 1
            log.exception("error in node logic of %s (%s)", nid, getattr(fn, "__name__", fn))

    # ------------------------------------------------------------------ main
    async def run(self) -> Path:
        """Run until the leader says stop. Returns this agent's event log path."""
        await self.transport.start([inbox_topic(n) for n in self.contexts] + [CONTROL_TOPIC])
        self._tasks += [asyncio.create_task(self._read_loop()),
                        asyncio.create_task(self._send_loop())]
        try:
            if self.leader:
                await self._lead_start()
            else:
                await self._follow_start()
            self._begin()
            if self.leader:
                try:
                    await asyncio.wait_for(self._stopped.wait(), self.experiment.duration_s)
                except TimeoutError:
                    pass
                await self._control({"type": "stop", "run_id": self.run_id})
            else:
                await self._stopped.wait()
        finally:
            await self._finish()
        return self.sink.path

    async def _lead_start(self) -> None:
        self._ready.update(self.contexts)
        self._check_ready()
        missing_ok = False
        try:
            await asyncio.wait_for(self._all_ready.wait(), self.ready_timeout_s)
        except TimeoutError:
            missing_ok = True
        if missing_ok:
            missing = sorted(self.expected - self._ready)
            log.warning("starting without %d node(s) that never said ready: %s%s",
                        len(missing), ", ".join(missing[:5]), " ..." if len(missing) > 5 else "")
        self.run_id = f"live-{uuid.uuid4().hex[:8]}"
        await self._control({"type": "start", "run_id": self.run_id})
        self._started.set()

    async def _follow_start(self) -> None:
        log.info("[%s] waiting for the coordinator's start signal...", self.name)
        while not self._started.is_set():
            await self._control({"type": "ready", "agent": self.name,
                                 "nodes": list(self.contexts)})
            try:
                await asyncio.wait_for(self._started.wait(), 1.0)
            except TimeoutError:
                pass

    def _begin(self) -> None:
        self._t0 = asyncio.get_running_loop().time()
        log.info("[%s] run %s started, hosting %d node(s)", self.name, self.run_id,
                 len(self.contexts))
        if self.leader:
            self.sink.emit(RunStarted(
                t=0.0, node=COORDINATOR_ID, run_id=self.run_id or "", runtime="live",
                experiment=self.experiment.name, seed=self.seed,
                orchestration=self.experiment.orchestration.name,
                duration_s=self.experiment.duration_s))
        for ctx in self.contexts.values():
            ctx.alive = True
        for nid, ctx in self.contexts.items():
            dev = self._device(nid)
            if dev is not None:
                ctx.emit(DeviceJoined(t=0.0, node=nid, device_id=nid, peer_id=dev.peer_id,
                                      cell_id=dev.cell_id))
            ctx.call(ctx.logic.on_start)
            if dev is not None and self.mobility is not None:
                self._tasks.append(asyncio.create_task(self._mobility_loop(ctx, dev)))
        pending, self._pending = self._pending, []
        for msg in pending:
            self._deliver(msg)

    async def _finish(self) -> None:
        for ctx in self.contexts.values():
            ctx.shutdown()
        if self.leader and self.run_id:
            self.sink.emit(RunEnded(t=self.now(), node=COORDINATOR_ID, run_id=self.run_id))
        try:  # let queued messages go out
            await asyncio.wait_for(self._outbox.join(), 2.0)
        except TimeoutError:
            pass
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.transport.close()
        self.sink.close()
        log.info("[%s] stopped; events in %s", self.name, self.sink.path)

    # ------------------------------------------------------------------ loops
    async def _read_loop(self) -> None:
        async for topic, payload in self.transport.incoming():
            if topic == CONTROL_TOPIC:
                self._on_control(json.loads(payload))
                continue
            nid = node_of_topic(topic)
            if nid not in self.contexts:
                continue
            try:
                msg = Message.from_json(payload)
            except ValueError:
                log.warning("bad message on %s: %.100s", topic, payload)
                continue
            if self._t0 is None:
                self._pending.append(msg)
            else:
                self._deliver(msg)

    def _deliver(self, msg: Message) -> None:
        ctx = self.contexts.get(msg.dst)
        if ctx is not None and ctx.alive:  # departed devices silently drop messages
            ctx.call(ctx.logic.on_message, msg)

    async def _send_loop(self) -> None:
        while True:
            msg = await self._outbox.get()
            try:
                await self.transport.publish(inbox_topic(msg.dst), msg.to_json())
            except Exception:
                log.exception("could not publish %s to %s", msg.kind, msg.dst)
            finally:
                self._outbox.task_done()

    async def _control(self, body: dict[str, Any]) -> None:
        await self.transport.publish(CONTROL_TOPIC, json.dumps(body))

    def _on_control(self, body: dict[str, Any]) -> None:
        kind = body.get("type")
        if kind == "ready" and self.leader:
            self._ready.update(body.get("nodes", []))
            self._check_ready()
            if self._started.is_set():  # a late agent missed "start": tell it again
                asyncio.get_running_loop().create_task(
                    self._control({"type": "start", "run_id": self.run_id}))
        elif kind == "start" and not self.leader and not self._started.is_set():
            self.run_id = body.get("run_id")
            self._started.set()
        elif kind == "stop" and not self.leader:
            self._stopped.set()
        elif kind == "availability" and body.get("agent") != self.name:
            self.registry.set_available(body["device_id"], bool(body["available"]))

    def _check_ready(self) -> None:
        if self.expected <= self._ready:
            self._all_ready.set()

    # ------------------------------------------------------------------ mobility
    def _device(self, nid: str) -> Device | None:
        if nid == COORDINATOR_ID:
            return None
        try:
            return self.registry.device(nid)
        except KeyError:
            return None

    async def _mobility_loop(self, ctx: LiveContext, dev: Device) -> None:
        """Device leaves after residence_time_s, maybe comes back after rejoin_delay_s."""
        assert self.mobility is not None
        rng = np.random.default_rng([self.seed, *b"mobility", *dev.id.encode()])
        while True:
            await asyncio.sleep(self.mobility.residence_time_s(dev, rng))
            graceful = self.mobility.is_graceful(dev, rng)
            ctx.emit(DeviceLeft(t=self.now(), node=dev.id, device_id=dev.id, graceful=graceful))
            ctx.call(ctx.logic.on_depart, graceful)
            ctx.shutdown()
            await self._set_available(dev.id, False)
            delay = self.mobility.rejoin_delay_s(dev, rng)
            if delay is None:
                return
            await asyncio.sleep(delay)
            await self._set_available(dev.id, True)
            ctx.alive = True
            ctx.emit(DeviceJoined(t=self.now(), node=dev.id, device_id=dev.id,
                                  peer_id=dev.peer_id, cell_id=dev.cell_id))
            ctx.call(ctx.logic.on_start)

    async def _set_available(self, device_id: str, available: bool) -> None:
        self.registry.set_available(device_id, available)
        await self._control({"type": "availability", "agent": self.name,
                             "device_id": device_id, "available": available})
