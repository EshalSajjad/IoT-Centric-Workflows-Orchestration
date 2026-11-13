"""Transports: how raw text gets from one process to another.

A transport only moves strings between named topics. It knows nothing about Messages,
logic or events (that is the agent's job). Two implementations:

    MqttTransport   the real thing: talks to a Mosquitto broker with aiomqtt
    LocalBroker /   an in-memory "broker" inside one Python process. Used by the tests
    LocalTransport  (so CI needs no Mosquitto) and by `orchflow run --broker memory`.

Topics (from core/messages.py):
    orchflow/{node_id}/in    a node's inbox. Every Message to that node goes here.
    orchflow/_control        runtime housekeeping (ready / start / stop / availability).
                             NOT protocol messages, so they are never counted or logged.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Sequence
from typing import Protocol

log = logging.getLogger(__name__)

CONTROL_TOPIC = "orchflow/_control"


def inbox_topic(node_id: str) -> str:
    return f"orchflow/{node_id}/in"


def node_of_topic(topic: str) -> str | None:
    """'orchflow/peer-01/in' -> 'peer-01'; anything else -> None."""
    parts = topic.split("/")
    if len(parts) == 3 and parts[0] == "orchflow" and parts[2] == "in":
        return parts[1]
    return None


class Transport(Protocol):
    async def start(self, topics: Sequence[str]) -> None:
        """Connect and subscribe. When this returns, messages to ``topics`` are received."""
        ...

    async def publish(self, topic: str, payload: str) -> None: ...

    def incoming(self) -> AsyncIterator[tuple[str, str]]:
        """Yields (topic, payload) for every message on a subscribed topic."""
        ...

    async def close(self) -> None: ...


# =========================================================================== MQTT


class MqttTransport:
    """Real MQTT over a Mosquitto broker. QoS 1 = the broker retries until delivered once."""

    def __init__(self, host: str = "localhost", port: int = 1883, client_id: str | None = None,
                 connect_attempts: int = 30) -> None:
        self.host, self.port, self.client_id = host, port, client_id
        self.connect_attempts = connect_attempts
        self._stack = contextlib.AsyncExitStack()
        self._client = None  # aiomqtt.Client once connected

    async def start(self, topics: Sequence[str]) -> None:
        import aiomqtt  # only needed for the live runtime (optional dependency "live")

        # The broker may still be booting (e.g. in docker compose): retry for a while.
        for attempt in range(1, self.connect_attempts + 1):
            client = aiomqtt.Client(self.host, self.port, identifier=self.client_id)
            try:
                self._client = await self._stack.enter_async_context(client)
                break
            except aiomqtt.MqttError as e:
                if attempt == self.connect_attempts:
                    raise ConnectionError(
                        f"cannot reach MQTT broker at {self.host}:{self.port}: {e}") from e
                log.info("broker %s:%s not ready (%s), retrying...", self.host, self.port, e)
                await asyncio.sleep(1.0)
        for t in topics:
            await self._client.subscribe(t, qos=1)  # type: ignore[union-attr]

    async def publish(self, topic: str, payload: str) -> None:
        assert self._client is not None, "call start() first"
        await self._client.publish(topic, payload, qos=1)

    async def incoming(self) -> AsyncIterator[tuple[str, str]]:
        assert self._client is not None, "call start() first"
        async for m in self._client.messages:
            raw = m.payload
            text = raw.decode() if isinstance(raw, bytes | bytearray) else str(raw)
            yield str(m.topic), text

    async def close(self) -> None:
        await self._stack.aclose()
        self._client = None


# =========================================================================== in-memory


class LocalBroker:
    """A tiny in-process stand-in for Mosquitto: topic -> subscribers' queues."""

    def __init__(self) -> None:
        self._subs: dict[str, list[asyncio.Queue[tuple[str, str]]]] = {}

    def transport(self) -> LocalTransport:
        return LocalTransport(self)

    def _subscribe(self, topic: str, q: asyncio.Queue[tuple[str, str]]) -> None:
        self._subs.setdefault(topic, []).append(q)

    def _unsubscribe_all(self, q: asyncio.Queue[tuple[str, str]]) -> None:
        for qs in self._subs.values():
            if q in qs:
                qs.remove(q)

    def _publish(self, topic: str, payload: str) -> None:
        for q in self._subs.get(topic, []):
            q.put_nowait((topic, payload))


class LocalTransport:
    def __init__(self, broker: LocalBroker) -> None:
        self._broker = broker
        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

    async def start(self, topics: Sequence[str]) -> None:
        for t in topics:
            self._broker._subscribe(t, self._queue)

    async def publish(self, topic: str, payload: str) -> None:
        self._broker._publish(topic, payload)
        await asyncio.sleep(0)  # let receivers run, like a real network would

    async def incoming(self) -> AsyncIterator[tuple[str, str]]:
        while True:
            yield await self._queue.get()

    async def close(self) -> None:
        self._broker._unsubscribe_all(self._queue)
