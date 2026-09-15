import json
import math
import random

import pytest

from fraud_pipeline.data import batch_rows, synthetic
from fraud_pipeline.domain import FEATURE_NAMES, FeatureEngine, Transaction, features


def event(identifier, timestamp, amount=10, entity="a", **kwargs):
    return Transaction(identifier, entity, timestamp, amount, **kwargs)


def reference(events):
    """Independent brute-force oracle for ordered, unique events."""
    outputs = []
    for i, current in enumerate(events):
        past = [
            row
            for row in events[:i]
            if row.entity_id == current.entity_id
            and current.timestamp - 3600 <= row.timestamp < current.timestamp
        ]
        values = {}
        for seconds, label in [(60, "1m"), (300, "5m"), (3600, "1h")]:
            window = [row for row in past if row.timestamp >= current.timestamp - seconds]
            values[f"count_{label}"] = len(window)
            values[f"spend_{label}"] = sum(row.amount for row in window)
        values["mean_5m"] = values["spend_5m"] / values["count_5m"] if values["count_5m"] else None
        values["max_1h"] = max((r.amount for r in past), default=None)
        values["merchants_1h"] = len({r.merchant_id for r in past if r.merchant_id})
        previous = past[-1] if past else None
        values["seconds_since_previous"] = current.timestamp - previous.timestamp if previous else None
        distance = None
        if previous and previous.latitude is not None and current.latitude is not None:
            # Independent spherical cosine formula, versus production haversine.
            a, b = math.radians(previous.latitude), math.radians(current.latitude)
            cos = math.sin(a) * math.sin(b) + math.cos(a) * math.cos(b) * math.cos(
                math.radians(current.longitude - previous.longitude)
            )
            distance = 6371.0088 * math.acos(min(1, max(-1, cos)))
        values["distance_km"] = distance
        values["speed_kmh"] = (
            distance / values["seconds_since_previous"] * 3600 if distance is not None else None
        )
        outputs.append(values)
    return outputs


def test_window_boundaries_and_equal_time():
    history = [event("1", 0, 5), event("2", 3540, 10), event("3", 3599, 20), event("4", 3600, 99)]
    result = features(history, event("5", 3600))
    assert result["count_1m"] == 2
    assert result["spend_1m"] == 30
    assert result["count_1h"] == 3
    assert result["spend_1h"] == 35
    assert result["seconds_since_previous"] == 1


def test_missing_history_and_geo():
    result = features([], event("1", 100))
    assert len(result) == 12
    assert result["count_1m"] == 0
    assert result["mean_5m"] is None
    assert result["distance_km"] is None


def test_duplicates_late_and_entity_isolation():
    engine = FeatureEngine()
    assert engine.process(event("1", 100))[0] == "accepted"
    assert engine.process(event("1", 100))[0] == "duplicate"
    assert engine.process(event("2", 99))[0] == "late"
    assert engine.process(event("3", 101, entity="b"))[1]["features"]["count_1m"] == 0
    assert engine.process(event("4", 101))[1]["features"]["count_1m"] == 1


@pytest.mark.parametrize("seed", range(10))
def test_independent_oracle_and_batch_parity(seed):
    events = list(synthetic(150, 5, seed=seed, rate=0.1))
    engine = FeatureEngine()
    live = [engine.process(row)[1] for row in events]
    assert live == list(batch_rows(events))
    for actual, expected in zip(live, reference(events), strict=True):
        for name in FEATURE_NAMES:
            if expected[name] is None:
                assert actual["features"][name] is None
            else:
                assert actual["features"][name] == pytest.approx(expected[name], rel=1e-5, abs=1e-5)


@pytest.mark.parametrize("seed", range(100))
def test_checkpoint_roundtrip_and_discard_uncommitted_state(seed):
    """Simulated checkpoint boundary; not a Kafka process-kill trial."""
    events = list(synthetic(60, 5, seed=seed))
    cut = random.Random(seed).randrange(1, len(events) - 1)
    engine = FeatureEngine()
    prefix = [engine.process(row)[1] for row in events[:cut]]
    committed_state = json.loads(json.dumps(engine.state))
    watermark = engine.watermark
    engine.process(events[cut])  # This mutation is lost before the hypothetical commit.
    restored = FeatureEngine(committed_state, watermark)
    outputs = prefix + [restored.process(row)[1] for row in events[cut:]]
    assert outputs == list(batch_rows(events))


def test_expiry_and_global_lateness():
    engine = FeatureEngine()
    engine.process(event("1", 0))
    engine.process(event("2", 90000, entity="b"))
    assert engine.expire() == ["a"]
    assert engine.process(event("3", 100, entity="c"))[0] == "late"


def edge_trace():
    """Sorted input; acceptance is hand-selected, never inferred from the engine."""
    return [
        event("shared", 0, 2, merchant_id="m1"),
        event("shared", 0, 50, entity="b"),
        event("peer", 0, 3, merchant_id="m2"),
        event("idle", 0, 500, entity="c"),
        event("shared", 0, 999),  # Duplicate with conflicting payload.
        event("minute", 60, 5, merchant_id="m1"),
        event("five-minutes", 300, 7),
        event("hour", 3600, 11, merchant_id="m3"),
        event("equal-hour", 3600, 13, merchant_id="m4"),
        event("gap", 3601, 17, entity="b"),
        event("shared", 86400, 999),  # Exact inclusive deduplication cutoff.
        event("shared", 86401, 19),  # Outside retention; engine accepts ID reuse.
        event("following", 86402, 23),
    ]


@pytest.mark.parametrize("partitions", [(0, 0, 0), (0, 1, 0), (0, 1, 2)])
@pytest.mark.parametrize("cut", range(14))
def test_edge_trace_partitioned_parity_and_checkpoint_cuts(partitions, cut):
    """In-process partition/checkpoint simulation, not a Kafka recovery trial."""
    events = edge_trace()
    accepted = [row for i, row in enumerate(events) if i not in (4, 10)]
    expected = reference(accepted)
    batch = list(batch_rows(events))
    assert [(row["entity_id"], row["transaction_id"], row["timestamp"]) for row in batch] == [
        (row.entity_id, row.transaction_id, row.timestamp) for row in accepted
    ]
    # Batch and stream share implementation; the independent oracle is essential.
    for snapshot, values in zip(batch, expected, strict=True):
        assert snapshot["features"] == pytest.approx(values)

    assignment = dict(zip(("a", "b", "c"), partitions, strict=True))
    engines = {partition: FeatureEngine() for partition in set(partitions)}
    live = []
    for i in range(len(events) + 1):
        if i == cut:
            for partition, engine in engines.items():
                engine.expire()
                engines[partition] = FeatureEngine(
                    json.loads(json.dumps(engine.state)), engine.watermark
                )
        if i == len(events):
            break
        engine = engines[assignment[events[i].entity_id]]
        status, snapshot = engine.process(events[i])
        assert status == ("duplicate" if i in (4, 10) else "accepted")
        if snapshot is not None:
            live.append(snapshot)
        if (i + 1) % 3 == 0:  # Expire at simulated worker batch boundaries.
            for active in engines.values():
                active.expire()
    assert live == batch


def test_batch_rejects_cross_entity_timestamp_regression():
    events = [event("first", 100, entity="a"), event("second", 99, entity="b")]
    engine = FeatureEngine()
    assert [engine.process(row)[0] for row in events] == ["accepted", "accepted"]
    with pytest.raises(ValueError, match="globally sorted by timestamp"):
        list(batch_rows(events))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"amount": -1},
        {"amount": float("nan")},
        {"currency": "EUR"},
        {"latitude": 30},
        {"latitude": 91, "longitude": 0},
        {"timestamp": True},
        {"entity_id": ""},
    ],
)
def test_invalid_contract(kwargs):
    data = {"transaction_id": "id", "entity_id": "a", "timestamp": 1, "amount": 2}
    data.update(kwargs)
    with pytest.raises(ValueError):
        Transaction(**data)
