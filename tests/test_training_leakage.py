"""Feature availability checks; synthetic fixtures are not model-quality evidence."""

import csv

import numpy as np

from fraud_pipeline.data import batch_rows, ieee_events
from fraud_pipeline.domain import MODEL_COLUMNS
from fraud_pipeline.model import matrix


def write_rows(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def snapshots(path):
    return list(batch_rows(event for event, _ in ieee_events(path)))


def fixture_rows():
    return [
        {
            "TransactionID": i,
            "TransactionDT": timestamp,
            "TransactionAmt": amount,
            "card1": 7,
            "isFraud": i % 2,
            "unavailable_after_review": i % 2,
        }
        for i, (timestamp, amount) in enumerate([(100, 10), (160, 20), (160, 30), (220, 40)])
    ]


def test_labels_and_unused_columns_do_not_change_model_inputs(tmp_path):
    original, changed = tmp_path / "original.csv", tmp_path / "changed.csv"
    rows = fixture_rows()
    write_rows(original, rows)
    write_rows(changed, [dict(row, isFraud=1 - row["isFraud"], unavailable_after_review=999) for row in rows])
    assert [label for _, label in ieee_events(original)] != [label for _, label in ieee_events(changed)]
    np.testing.assert_array_equal(matrix(snapshots(original)), matrix(snapshots(changed)))


def test_future_rows_cannot_change_prefix_features(tmp_path):
    prefix, extended = tmp_path / "prefix.csv", tmp_path / "extended.csv"
    rows = fixture_rows()
    write_rows(prefix, rows)
    future = dict(rows[-1], TransactionID=99, TransactionDT=280, TransactionAmt=999999)
    # Scrambled file order also checks that the adapter sorts before replay.
    write_rows(extended, [future, *reversed(rows)])
    assert snapshots(prefix) == snapshots(extended)[:len(rows)]


def test_ieee_inputs_exclude_current_and_equal_time_amounts_from_history(tmp_path):
    source = tmp_path / "source.csv"
    write_rows(source, fixture_rows())
    rows = snapshots(source)
    for snapshot in rows[1:3]:
        assert snapshot["features"]["count_1m"] == 1
        assert snapshot["features"]["spend_1m"] == 10
    assert rows[3]["features"]["count_1m"] == 2
    assert rows[3]["features"]["spend_1m"] == 50
    assert matrix(rows).shape == (4, len(MODEL_COLUMNS))
    for snapshot in rows:
        assert snapshot["features"]["merchants_1h"] == 0
        assert snapshot["features"]["distance_km"] is None
        assert snapshot["features"]["speed_kmh"] is None
