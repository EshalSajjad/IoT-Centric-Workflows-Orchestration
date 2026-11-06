"""Plug-in registry: how experiment YAML picks implementations by name.

SHARED CONTRACT.

    # policies/binding_nearest.py  (Track 1)
    from orchflow.core.plugins import register

    @register("binding", "nearest")
    class NearestBinding: ...

    # anywhere (runtime / CLI)
    policy = create("binding", experiment.binding)   # PluginRef(name="nearest", params={})

Kinds: binding, orchestration, mobility, network, work.
A future student adds an idea = one new file with @register + one line of YAML.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Literal, TypeVar

from orchflow.core.spec import PluginRef

PluginKind = Literal["binding", "orchestration", "mobility", "network", "work"]
PLUGIN_KINDS: tuple[PluginKind, ...] = ("orchestration", "binding", "mobility", "network", "work")

# Modules imported on first use so their @register decorators run. Each track appends its
# own modules here (no contract PR needed for this list -- just add your line).
_BUILTIN_MODULES: list[str] = [
    "orchflow.core.testing",  # "echo" orchestration + "first" binding (fakes)
    # Track 1: "orchflow.policies.gom", "orchflow.policies.lom",
    #          "orchflow.policies.binding_nearest", "orchflow.policies.binding_qos",
    # Track 2: "orchflow.runtime.sim.mobility", "orchflow.runtime.sim.network",
    #          "orchflow.runtime.sim.work",
]

_REGISTRY: dict[str, dict[str, Callable[..., Any]]] = {}
_loaded = False

T = TypeVar("T", bound=Callable[..., Any])


def register(kind: PluginKind, name: str) -> Callable[[T], T]:
    def deco(factory: T) -> T:
        table = _REGISTRY.setdefault(kind, {})
        if name in table and table[name] is not factory:
            raise ValueError(f"{kind} plug-in {name!r} registered twice")
        table[name] = factory
        return factory

    return deco


def _load_builtins() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    for mod in _BUILTIN_MODULES:
        try:
            importlib.import_module(mod)
        except ModuleNotFoundError as e:  # a track's module not written yet
            if e.name != mod:
                raise


def available(kind: PluginKind) -> list[str]:
    _load_builtins()
    return sorted(_REGISTRY.get(kind, {}))


def create(kind: PluginKind, ref: PluginRef | str, **extra: Any) -> Any:
    """Instantiate plug-in ``ref.name`` of ``kind`` with ``ref.params`` (+ ``extra``)."""
    _load_builtins()
    if isinstance(ref, str):
        ref = PluginRef(name=ref)
    try:
        factory = _REGISTRY[kind][ref.name]
    except KeyError:
        raise KeyError(
            f"no {kind} plug-in named {ref.name!r}; available: {available(kind)}"
        ) from None
    return factory(**ref.params, **extra)
