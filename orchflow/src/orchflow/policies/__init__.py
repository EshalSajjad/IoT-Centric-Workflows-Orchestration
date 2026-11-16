"""TRACK 1 (logic): GOM/LOM node logic + binding policies.

Files to create: gom.py, lom.py, binding_nearest.py, binding_qos.py (+ core/registry.py,
workflows/imw.yaml via the legacy-XML importer).
Implement OrchestrationProtocol / NodeLogic / BindingPolicy from orchflow.core.interfaces,
register with @orchflow.core.plugins.register, add the module to plugins._BUILTIN_MODULES.
Test with orchflow.core.testing.RecordingContext.
"""
