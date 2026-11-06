"""TRACK 3 (live): asyncio + MQTT (Mosquitto) runtime.

Files to create: mqtt_transport.py, agent.py (one process hosts one or many nodes),
entry points for coordinator / peer / device.
Implement orchflow.core.interfaces.NodeContext over MQTT (topic orchflow/{node_id}/in).
Test with the "echo" orchestration from orchflow.core.testing.
"""
