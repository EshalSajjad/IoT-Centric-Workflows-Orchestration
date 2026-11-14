"""Track 1: the "legacy" mobility plug-in (departures as the original code decided them)."""

from __future__ import annotations

import numpy as np
import pytest

from orchflow.core.models import Device, Location
from orchflow.core.plugins import create
from orchflow.core.spec import PluginRef
from orchflow.policies.mobility_legacy import RESIDENCE_TABLE_MIN, LegacyMobility, stays

LOC = Location(lat=30.456048, lon=-91.205240)


def dev(n: int) -> Device:
    return Device(id=f"dev-{n:04d}", name=f"Device_{n}", peer_id="peer-01", cell_id="cell-01",
                  location=LOC)


def test_one_check_matches_the_java_formula() -> None:
    # r = 7 min: p = 1 - 0.2 e^-1.4 = 0.951 -> round(9.51) = 10 -> every draw stays
    assert all(stays(7, d) for d in range(11))
    # r = 0: p = 0.8 -> 8: draws 9 and 10 leave
    assert [stays(0, d) for d in range(11)] == [True] * 9 + [False] * 2
    # Java rounds half up: r with 10p = 8.5 exactly must give 9, not Python's 8
    r = -5 * np.log(0.75)  # 1 - 0.2 e^(-0.2 r) = 0.85
    assert stays(r, 9) and not stays(r, 10)
    assert not stays(-1e9, 0)  # far past the residence time: always leaves, no overflow


def test_check_intervals_are_legacy() -> None:
    assert LegacyMobility(mode="gom").interval_s == 110.0  # Mobility_IoT: sleep(110000)
    assert LegacyMobility(mode="lom").interval_s == 30.0  # Mobility_LOM.java:93


@pytest.mark.parametrize("mode", ["gom", "lom"])
def test_departures_fall_on_check_times_and_are_seeded(mode: str) -> None:
    def lives(seed: int) -> list[float]:
        m = LegacyMobility(mode=mode)
        rng = np.random.default_rng(seed)
        return [m.residence_time_s(dev(n), rng) for n in range(1, 21) for _ in range(2)]

    a = lives(1)
    assert a == lives(1)
    interval = LegacyMobility(mode=mode).interval_s
    assert all(t % interval == 0 for t in a)
    # the decaying residence means no device outlives its table value by much
    assert max(a) / 60 <= max(RESIDENCE_TABLE_MIN)


def test_first_life_uses_the_table_then_new_devices_get_10_to_15_min() -> None:
    m = LegacyMobility(mode="lom")
    rng = np.random.default_rng(0)
    assert m._start_minutes(dev(8), rng) == RESIDENCE_TABLE_MIN[7]  # slot 8 -> 5 min
    assert m._start_minutes(dev(28), rng) == RESIDENCE_TABLE_MIN[7]  # slots wrap every 20
    assert all(10 <= m._start_minutes(dev(8), rng) <= 15 for _ in range(50))


def test_registered_and_validated() -> None:
    m = create("mobility", PluginRef(name="legacy", params={"mode": "lom"}))
    assert isinstance(m, LegacyMobility)
    assert m.rejoin_delay_s(dev(1), np.random.default_rng(0)) == 160.0
    rng = np.random.default_rng(0)
    assert LegacyMobility(mode="gom").rejoin_delay_s(dev(1), rng) == 250.0
    assert LegacyMobility(rejoin_delay_s=None).rejoin_delay_s(dev(1), rng) is None
    assert not m.is_graceful(dev(1), np.random.default_rng(0))
    with pytest.raises(ValueError):
        LegacyMobility(mode="xom")
    with pytest.raises(ValueError):
        LegacyMobility(new_device_min=(15, 10))
