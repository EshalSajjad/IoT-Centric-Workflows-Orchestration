"""Mobility plug-in "legacy": when devices leave, as the ORIGINAL Orchflow code decided it.

The paper says residence times were exponential (mean 5 min); that is Track 2's
``exponential`` plug-in. The original code did something else, and this plug-in copies it so
both can be run and compared (docs/deviations.md D4):

* Each device slot starts with a residence time from a fixed table, in minutes
  (``Mobility_LOM.java:76``, ``Mobility_GOM.java:42``, ``Mobility_IoT`` in end-device.jar).
* Every check, the remaining time ``r`` is reduced and the device stays with
  ``p = 1 - 0.2 * exp(-0.2 * r)``: draw ``value`` uniformly from 0..10, stay if
  ``value <= round(10 * p)`` (``Mobility_LOM.java:257-310``).
  Legacy subtracts the time since the slot's start **at every check** (``:263-275``), not the
  time since the last check, so ``r`` falls faster and faster. We keep that: it is what decided
  departure times in the original runs.
* Check interval: GOM 110 s (the end device decides, ``Mobility_IoT.run``: ``sleep(110000)``,
  then stops its "I am alive"); LOM 30 s (the peer decides, ``Mobility_LOM.java:93``).
* After a departure a new device takes the slot with 10-15 min (``Add_New_Device``,
  ``Mobility_LOM.java:927-939``), so the pool never empties. Here: the same device id
  rejoins after ``rejoin_delay_s`` (default GOM 250 s, LOM 160 s). It can't be immediate:
  it must exceed the worst detection time (heartbeat timeout + check interval), otherwise
  the returning device's heartbeat hides the departure from its peer.

Simplified, see deviations.md D4: legacy also restarted every slot's clock after an
announcement (``Reset_Device``) and when a device was picked as a replacement
(``Main_Rasp_Combine.java:183,191``); the mobility interface can't see either, so we don't.
Legacy only acted on departures of devices holding a deployment; with nearest binding the
other devices are only spare candidates, and they come back after ``rejoin_delay_s``.
"""

from __future__ import annotations

import math
import re

import numpy as np

from orchflow.core.models import Device
from orchflow.core.plugins import register

# minutes, per slot (Mobility_LOM.java:76 / Mobility_GOM.java:42; same in Mobility_IoT)
RESIDENCE_TABLE_MIN = (7, 7, 10, 10, 10, 9, 12, 5, 15, 6, 12, 6, 12, 10, 12, 7, 15, 10, 7, 14)
CHECK_INTERVAL_S = {"gom": 110.0, "lom": 30.0}
# just over the worst detection time (heartbeat timeout 120 s + the peer's check interval:
# GOM 120 s, LOM 30 s), so the returning device's heartbeat can't hide its departure
REJOIN_DELAY_S = {"gom": 250.0, "lom": 160.0}
LAMBDA = 0.2


def _java_round(x: float) -> int:
    """Java Math.round: half up (Python's round() is half to even)."""
    return math.floor(x + 0.5)


def stays(remaining_min: float, draw: int) -> bool:
    """One legacy check: does the device stay? ``draw`` is uniform in 0..10."""
    if remaining_min < -40:  # p is hugely negative: always leaves (and avoids overflow)
        return False
    p = 1 - LAMBDA * math.exp(-LAMBDA * remaining_min)
    return draw <= _java_round(p * 10)


@register("mobility", "legacy")
class LegacyMobility:
    def __init__(self, mode: str = "gom", check_interval_s: float | None = None,
                 new_device_min: tuple[int, int] = (10, 15),
                 rejoin_delay_s: float | None = -1.0, max_checks: int = 10_000) -> None:
        if mode not in CHECK_INTERVAL_S:
            raise ValueError(f"mode must be one of {sorted(CHECK_INTERVAL_S)}, not {mode!r}")
        lo, hi = new_device_min
        if not 0 < lo <= hi:
            raise ValueError("new_device_min must be (lo, hi) with 0 < lo <= hi")
        if rejoin_delay_s is not None and rejoin_delay_s < 0:
            rejoin_delay_s = REJOIN_DELAY_S[mode]  # default: per mode
        self.interval_s = check_interval_s or CHECK_INTERVAL_S[mode]
        if self.interval_s <= 0:
            raise ValueError("check_interval_s must be positive")
        self.new_device_min = (lo, hi)
        self.rejoin_delay = rejoin_delay_s
        self.max_checks = max_checks
        self._lives: dict[str, int] = {}

    def _start_minutes(self, device: Device, rng: np.random.Generator) -> float:
        life = self._lives.get(device.id, 0)
        self._lives[device.id] = life + 1
        if life == 0:
            digits = re.findall(r"\d+", device.id)
            slot = (int(digits[-1]) - 1) if digits else 0
            return float(RESIDENCE_TABLE_MIN[slot % len(RESIDENCE_TABLE_MIN)])
        lo, hi = self.new_device_min
        return float(rng.integers(lo, hi + 1))

    def residence_time_s(self, device: Device, rng: np.random.Generator) -> float:
        remaining = self._start_minutes(device, rng)
        for k in range(self.max_checks):  # legacy checks first, then sleeps
            elapsed_min = k * self.interval_s / 60
            remaining -= elapsed_min  # (sic) time since start, subtracted every check
            if not stays(remaining, int(rng.integers(0, 11))):
                return k * self.interval_s
        return self.max_checks * self.interval_s

    def rejoin_delay_s(self, device: Device, rng: np.random.Generator) -> float | None:
        return self.rejoin_delay

    def is_graceful(self, device: Device, rng: np.random.Generator) -> bool:
        return False  # legacy devices never announced leaving
