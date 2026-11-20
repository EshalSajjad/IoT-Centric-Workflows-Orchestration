"""The dashboard process: listen to a run's messages, serve the map page.

    orchflow dashboard EXPERIMENT.yaml                         # run it here + show it (no broker)
    orchflow dashboard EXPERIMENT.yaml --broker localhost:1883 # watch a run on a real broker
    orchflow dashboard EXPERIMENT.yaml --map osm               # OpenStreetMap instead of Google
    orchflow dashboard EXPERIMENT.yaml --plume computed        # plume from the live readings

The map is Google Maps, like legacy. Without a key Google shows it darkened with "For
development purposes only", exactly as in the legacy demo; set ORCHFLOW_GOOGLE_MAPS_KEY to
your own key to remove that (the key is read from the environment, never stored in the repo).

With ``--broker memory`` (the default) the whole experiment runs inside this process on the
in-memory broker, so one command shows everything. With a real broker the dashboard only
watches: start the agents separately (`orchflow agent ...` / docker compose / the VMs).
Open http://localhost:8050.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from orchflow.core.messages import Message
from orchflow.runtime.live.agent import (
    LiveAgent,
    build_registry,
    load_inputs,
    load_mobility,
    select_nodes,
)
from orchflow.runtime.live.transport import (
    CONTROL_TOPIC,
    LocalBroker,
    MqttTransport,
    Transport,
    inbox_topic,
)
from orchflow.viz.kml import ALOHA_IMAGE
from orchflow.viz.state import DashboardState

log = logging.getLogger("orchflow.viz")
PAGE = Path(__file__).with_name("page.html")
MAPS = ("google", "osm")
KEY_ENV = "ORCHFLOW_GOOGLE_MAPS_KEY"


def render_page(map_provider: str, google_key: str | None) -> bytes:
    config = json.dumps({"map": map_provider, "googleKey": google_key or ""})
    return PAGE.read_text(encoding="utf-8").replace("/*CONFIG*/{}", config).encode()


class Dashboard:
    def __init__(self, state: DashboardState, transport: Transport, *,
                 map_provider: str = "google", google_key: str | None = None) -> None:
        if map_provider not in MAPS:
            raise ValueError(f"map must be one of {MAPS}")
        self.state, self.transport = state, transport
        self._t0: float | None = None
        self._stopped_at: float | None = None  # the clock freezes when the run stops
        self.snapshot = b"{}"
        self.static = json.dumps(state.static()).encode()
        self.page = render_page(map_provider, google_key)

    def now(self) -> float:
        if self._stopped_at is not None:
            return self._stopped_at
        return 0.0 if self._t0 is None else time.monotonic() - self._t0

    async def listen(self, node_ids: list[str]) -> None:
        await self.transport.start([inbox_topic(n) for n in node_ids] + [CONTROL_TOPIC])
        async for topic, payload in self.transport.incoming():
            try:
                if topic == CONTROL_TOPIC:
                    body = json.loads(payload)
                    if body.get("type") == "start":
                        self._t0, self._stopped_at = time.monotonic(), None
                    elif body.get("type") == "stop":
                        # show the run as it ended: no devices "away" or cars moving afterwards
                        self._stopped_at = self.now()
                    self.state.on_control(body)
                    continue
                msg = Message.from_json(payload)
                if self._t0 is None:  # joined mid-run: start our clock now
                    self._t0 = time.monotonic()
                self.state.on_message(msg, self.now())
            except Exception:  # a bad message must not stop the dashboard
                log.exception("dashboard could not handle a message on %s", topic)

    async def publish_snapshots(self, every_s: float = 0.5) -> None:
        """Serialise the state on the event loop (where it changes); the HTTP thread only
        ever reads the finished bytes, so no locking is needed."""
        while True:
            self.snapshot = json.dumps(self.state.snapshot(self.now())).encode()
            await asyncio.sleep(every_s)


def _http_server(dash: Dashboard, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server's naming)
            if self.path.startswith("/state"):
                body, ctype = dash.snapshot, "application/json"
            elif self.path.startswith("/static"):
                body, ctype = dash.static, "application/json"
            elif self.path.startswith("/aloha.jpg"):
                body, ctype = ALOHA_IMAGE.read_bytes(), "image/jpeg"
            elif self.path in ("/", "/index.html"):
                body, ctype = dash.page, "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass  # no access log on the console

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def run_dashboard(experiment: Path, *, broker: str = "memory", port: int = 8050,
                        duration_s: float | None = None, seed: int = 0,
                        run_dir: Path | None = None, map_provider: str = "google",
                        plume_mode: str = "aloha") -> None:
    exp, wf, testbed = load_inputs(experiment, duration_s=duration_s)
    registry = build_registry(testbed, wf)
    nodes = select_nodes("all", registry)
    heartbeat = float(exp.orchestration.params.get("heartbeat_s", 60.0))
    state = DashboardState(wf, build_registry(testbed, wf), heartbeat_s=heartbeat,
                           plume_mode=plume_mode)
    look = {"map_provider": map_provider, "google_key": os.environ.get(KEY_ENV)}

    tasks: list[asyncio.Task[object]] = []
    if broker == "memory":
        hub = LocalBroker()
        dash = Dashboard(state, hub.transport(), **look)
        agent = LiveAgent(experiment=exp, workflow=wf, registry=registry, nodes=nodes,
                          transport=hub.transport(), run_dir=run_dir or Path("results/dashboard"),
                          seed=seed, name="all", mobility=load_mobility(exp),
                          ready_timeout_s=10)
    else:
        host, _, p = broker.partition(":")
        dash = Dashboard(state, MqttTransport(host, int(p or 1883),
                                              client_id=f"orchflow-dashboard-{time.time_ns()}"),
                         **look)
        agent = None

    server = _http_server(dash, port)
    print(f"dashboard: http://localhost:{port}  (Ctrl+C to stop)", flush=True)
    tasks.append(asyncio.create_task(dash.listen(nodes)))
    tasks.append(asyncio.create_task(dash.publish_snapshots()))
    await asyncio.sleep(0.1)  # subscribed before the agent starts sending
    try:
        if agent is not None:
            await agent.run()
            print("run finished; the dashboard stays up until Ctrl+C", flush=True)
        await asyncio.Event().wait()  # until Ctrl+C
    finally:
        for t in tasks:
            t.cancel()
        server.shutdown()
        with contextlib.suppress(Exception):
            await dash.transport.close()
