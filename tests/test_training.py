import csv
import json

import pytest

from fraud_pipeline.data import batch_rows, ieee_events
from fraud_pipeline.model import RiskModel, train


def test_train_save_load_and_chronological_split(tmp_path):
    source = tmp_path / "train_transaction.csv"
    with source.open("w", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=["TransactionID", "TransactionDT", "TransactionAmt", "card1", "isFraud"]
        )
        writer.writeheader()
        for i in range(240):
            writer.writerow(
                {
                    "TransactionID": i,
                    "TransactionDT": i * 10,
                    "TransactionAmt": 500 if i % 4 == 0 else 20,
                    "card1": i % 8,
                    "isFraud": int(i % 4 == 0),
                }
            )
    output = tmp_path / "model"
    report = train(source, output)
    assert sum(report["sizes"]) == 240
    assert report["cutoffs"][0] < report["cutoffs"][1]
    assert 0 <= report["xgboost"]["roc_auc"] <= 1
    model = RiskModel(output)
    snapshot = next(batch_rows(event for event, _ in ieee_events(source)))
    assert not model.demo
    assert 0 <= model.predict(snapshot) <= 1
    metadata = json.loads((output / "metadata.json").read_text())
    metadata["columns"] = []
    (output / "metadata.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="schema"):
        RiskModel(output)
