"""Hand-worked event-time boundaries, independent of batch/stream agreement."""

from copy import deepcopy
import json
import math

import pytest

from fraud_pipeline.domain import FeatureEngine, Transaction


def event(identifier, timestamp, amount=10, entity="a", **kwargs):
    return Transaction(identifier, entity, timestamp, amount, **kwargs)


@pytest.mark.parametrize("window,label", [(60, "1m"), (300, "5m"), (3600, "1h")])
def test_each_window_includes_lower_boundary_and_excludes_equal_time(window, label):
    engine = FeatureEngine()
    cutoff = 7200 - window
    for row in [
        event("outside", math.nextafter(cutoff, -math.inf), 100),
        event("boundary", cutoff, 2),
        event("inside", math.nextafter(cutoff, math.inf), 3),
        event("equal", 7200, 1000),
    ]:
        assert engine.process(row)[0] == "accepted"
    status, snapshot = engine.process(event("current", 7200, 10000))
    assert status == "accepted"
    assert snapshot["features"][f"count_{label}"] == 2
    assert snapshot["features"][f"spend_{label}"] == 5


def test_equal_time_peers_are_retained_and_previous_uses_input_order():
    engine = FeatureEngine()
    engine.process(event("first", 100, 2, latitude=0, longitude=0))
    _, peer = engine.process(event("peer", 100, 3, latitude=0, longitude=1))
    assert peer["features"]["count_1h"] == 0
    for name in ("seconds_since_previous", "distance_km", "speed_kmh"):
        assert peer["features"][name] is None
    _, later = engine.process(event("later", 101, latitude=0, longitude=1))
    assert later["features"]["count_1m"] == 2
    assert later["features"]["spend_1m"] == 5
    assert later["features"]["seconds_since_previous"] == 1
    assert later["features"]["distance_km"] == 0
    assert later["features"]["speed_kmh"] == 0


@pytest.mark.parametrize("age,status", [(86399, "duplicate"), (86400, "duplicate"), (86401, "accepted")])
def test_duplicate_horizon_is_inclusive_and_rejection_does_not_refresh_it(age, status):
    engine = FeatureEngine()
    engine.process(event("same", 100, 2))
    before = deepcopy((engine.state, engine.watermark, engine.expiry))
    actual, snapshot = engine.process(event("same", 100 + age, 999))
    assert actual == status
    if status == "duplicate":
        assert snapshot is None
        assert (engine.state, engine.watermark, engine.expiry) == before
        assert engine.process(event("same", 86501))[0] == "accepted"
    else:
        assert snapshot["features"]["count_1h"] == 0
        assert engine.state["a"]["seen"] == {"same": 86501}


@pytest.mark.parametrize("offset,status", [(-0.001, "late"), (0, "accepted"), (0.001, "accepted")])
def test_partition_lateness_boundary_for_another_entity(offset, status):
    engine = FeatureEngine()
    engine.process(event("same", 10000))
    before = deepcopy((engine.state, engine.watermark, engine.expiry))
    actual, snapshot = engine.process(event("same", 6400 + offset, entity="b"))
    assert actual == status
    assert engine.watermark == 10000
    if status == "late":
        assert snapshot is None
        assert (engine.state, engine.watermark, engine.expiry) == before
    else:
        assert snapshot["features"]["count_1h"] == 0


@pytest.mark.parametrize("identifier,timestamp,status", [
    ("fresh", 99, "late"),
    ("same", 99, "duplicate"),
    ("same", 0, "late"),
])
def test_rejection_precedence_and_no_state_mutation(identifier, timestamp, status):
    engine = FeatureEngine()
    engine.process(event("same", 100))
    engine.process(event("advance", 3601, entity="b"))
    before = deepcopy((engine.state, engine.watermark, engine.expiry))
    assert engine.process(event(identifier, timestamp)) == (status, None)
    assert (engine.state, engine.watermark, engine.expiry) == before


@pytest.mark.parametrize("restore", [False, True])
def test_expiry_strict_cutoff_and_stale_heap_entries(restore):
    engine = FeatureEngine()
    engine.process(event("old", 100))
    engine.process(event("new", 101))
    engine.process(event("peer", 101))
    engine.process(event("clock", 86501, entity="b"))
    if restore:
        engine = FeatureEngine(json.loads(json.dumps(engine.state)), engine.watermark)
    assert engine.expire() == []  # Old heap entry cannot delete refreshed state.
    assert "a" in engine.state  # Exactly 24 hours is retained.
    assert engine.expire() == []  # Wall-clock passage is irrelevant.
    engine.process(event("next", math.nextafter(86501, math.inf), entity="b"))
    assert engine.expire() == ["a"]  # Equal-time heap entries emit one tombstone.
    assert set(engine.state) == {"b"}
    assert engine.expire() == []
