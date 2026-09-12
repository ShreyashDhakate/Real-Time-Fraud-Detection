"""Run after docker compose up: verify Kafka -> Redis -> gRPC and offline parity."""

import json
from dataclasses import replace
import time
import uuid

from fraud_pipeline.config import Settings
from fraud_pipeline.data import batch_rows, synthetic
from fraud_pipeline.serving import score
from fraud_pipeline.store import FeatureStore
from fraud_pipeline.streaming import produce_events

settings = Settings()
entity_prefix = f"smoke-{uuid.uuid4().hex[:12]}-"
events = [
    replace(event, entity_id=entity_prefix + event.entity_id)
    for event in synthetic(20, 3, start=time.time() - 10, rate=100)
]
produce_events(settings, events)
store = FeatureStore(settings.redis_url, settings.namespace)
expected = list(batch_rows(events))
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    actual = [store.get(row.entity_id, row.transaction_id) for row in events]
    if all(actual):
        break
    time.sleep(0.5)
else:
    raise SystemExit("Snapshots not materialized in 60s; inspect worker/materializer logs")
for wanted, received in zip(expected, actual, strict=True):
    for key in wanted:
        assert received[key] == wanted[key], f"Parity failure: {key}"
response = score("localhost:50051", events[-1].entity_id, events[-1].transaction_id)
assert 0 <= response["risk_score"] <= 1
print(json.dumps({"accepted": len(events), "parity": "passed", "score": response}, indent=2))
