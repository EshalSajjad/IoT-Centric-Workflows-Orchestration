"""TRACK 3 (live): asyncio + MQTT (Mosquitto) runtime.

    transport.py  moves text between topics: MqttTransport (real) / LocalBroker (in-memory)
    context.py    LiveContext, the real-world NodeContext given to node logic
    agent.py      LiveAgent, one process hosting one or many nodes; start/stop barrier
    work.py       deploy / execute (placeholder: waits as long as the work would take)
    logs.py       per-agent event files and merging them into events.jsonl

Run it with the CLI (`orchflow agent ...`, `orchflow run ...`) or deploy/local/docker-compose.yml.
"""

from orchflow.runtime.live.agent import LiveAgent, build_registry, load_inputs, select_nodes
from orchflow.runtime.live.logs import merge_events
from orchflow.runtime.live.transport import LocalBroker, MqttTransport

__all__ = ["LiveAgent", "LocalBroker", "MqttTransport", "build_registry", "load_inputs",
           "merge_events", "select_nodes"]
