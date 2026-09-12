import hashlib
import json
import random
from pathlib import Path

from .domain import FeatureEngine, Transaction


def synthetic(count=1000, entities=100, seed=42, start=1_700_000_000.0, rate=100):
    rng = random.Random(seed)
    for i in range(count):
        yield Transaction(
            f"s{seed}-{start}-{i}",
            f"account-{rng.randrange(entities)}",
            start + i / rate,
            round(rng.lognormvariate(3, 1.2), 2),
            f"merchant-{rng.randrange(40)}",
            latitude=40 + rng.random() / 10,
            longitude=-74 + rng.random() / 10,
        )


def read_events(path):
    with open(path, encoding="utf-8") as source:
        for line in source:
            if line.strip():
                yield Transaction(**json.loads(line))


def write_jsonl(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row, allow_nan=False, sort_keys=True) + "\n")


def batch_rows(events):
    engine = FeatureEngine()
    last_timestamp = -1
    for event in events:
        if event.timestamp < last_timestamp:
            raise ValueError(
                "Batch input must be globally sorted by timestamp for partition-independent parity"
            )
        last_timestamp = event.timestamp
        status, snapshot = engine.process(event)
        if status == "late":
            raise ValueError("Batch input violates the streaming event-time ordering contract")
        if snapshot:
            yield snapshot
        engine.expire()


def ieee_events(path):
    """Explicit proxy adapter. No invented coordinates or real account identities."""
    import pandas as pd

    proxy_fields = ("card1", "card2", "card3", "card5", "addr1")
    required = {"TransactionID", "TransactionDT", "TransactionAmt", "isFraud"}
    rows = pd.read_csv(
        path,
        usecols=lambda col: col in required or col in proxy_fields,
        dtype={name: "string" for name in proxy_fields},
    )
    if not required.issubset(rows.columns):
        raise ValueError(f"Training CSV must contain {sorted(required)}")
    rows = rows.sort_values(["TransactionDT", "TransactionID"], kind="stable")
    for values in rows.itertuples(index=False):
        row = values._asdict()
        proxy = "|".join("" if pd.isna(row.get(k)) else str(row[k]) for k in proxy_fields)
        entity = hashlib.sha256(proxy.encode()).hexdigest()[:24]
        # IEEE-CIS amounts are used as dataset units. USD is the internal schema
        # convention, not a claim that the anonymized source includes currency.
        event = Transaction(
            str(row["TransactionID"]), entity, float(row["TransactionDT"]), float(row["TransactionAmt"])
        )
        label = row["isFraud"]
        if label not in (0, 1):
            raise ValueError("isFraud must be 0 or 1")
        yield event, int(label)
