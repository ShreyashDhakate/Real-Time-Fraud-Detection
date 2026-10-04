# Scoring failure contract and reproduction

Coverage review and local probes: 2026-09-16. This is a reliability specification
with a reproduced open defect, not a completed reliability fix. Tests use real
localhost gRPC transport, injected stores, and an explicitly untrained demo
model. They do not establish live Redis behavior or trained-model integration.

## Existing contract and coverage

`Score` looks up an immutable `(entity_id, transaction_id)` snapshot. Its
`feature_version` is source provenance (for example `0:1`), whereas
`schema_version` defines feature semantics. These are different identifiers.
`feature_age_seconds` measures wall-clock time minus event timestamp, not Kafka
lag or time since Redis materialization. Refreshing a key's TTL must not make an
old event scoreable.

| Case | Current behavior and test evidence | Remaining acceptance check |
| --- | --- | --- |
| Valid snapshot | Existing roundtrip returns a demo score and feature version; new boundary probes also check model version and exact age | Trained full-stack response must explicitly have `demo_model=false` and the expected artifact version |
| Empty or oversized identifiers | Handler rejects empty IDs or either ID over 200 characters with `INVALID_ARGUMENT`; existing test covers empty entity only | Empty transaction, 200/201-character boundaries, and no store call on invalid input |
| Missing or expired key | Existing injected-store test returns `NOT_FOUND` | Real Redis expiry and recovery after materialization |
| Stale event | `age > max_age` returns `FAILED_PRECONDITION`; existing test uses age about 90000 seconds | New frozen-clock probes pass at exactly 86400 and reject 86400.001, with no inference on rejection |
| Future event | `age < -60` returns `FAILED_PRECONDITION` | New probes accept exactly -60 with response age clamped to zero, and reject -60.001 without inference |
| Redis error | Handler maps `RedisError` to `UNAVAILABLE`; existing test injects a connection error | New connection/timeout probes verify no inference; actual refused connection, blocked socket, and Redis restart remain untested |
| Deadline expires in store read | New event-synchronized probe receives client `DEADLINE_EXCEEDED`; handler skips inference after GET returns | The read itself remains blocked until released; no deadline-aware Redis budget or worker-release latency guarantee is established |
| Model artifact mismatch | Existing synthetic training test verifies changed column metadata is rejected at model load; missing model requires explicit demo opt-in | Schema-number mismatch, checksum corruption, incomplete bundle, and startup readiness coverage |
| Snapshot schema mismatch | **Reproduced defect:** incompatible snapshot is scored successfully | Reject before inference with `FAILED_PRECONDITION`; remove the strict expected-failure marker with the fix |

The new transport tests are in `tests/test_scoring_contract.py`. The prior
coverage is in `tests/test_serving.py` and `tests/test_training.py`. Source review
also covered `serving.py`, `store.py`, `model.py`, the protobuf contract, the
architecture, and the integration smoke script.

## Selected correction: validate the snapshot at the serving boundary

The highest-priority verified gap from these probes is request-time schema
validation. `FeatureStore.put` rejects an incompatible schema and the Redis key
includes the current schema namespace, but `FeatureStore.get` only decodes JSON.
Neither `Scorer.Score` nor `RiskModel.predict` checks the retrieved snapshot's
schema version. A wrong payload under a current key can therefore bypass the
write-path guard. Model metadata validation at startup does not validate each
snapshot. This is a defensive read-path defect reproduced with an injected
payload; no live Redis corruption is claimed.

Reproducer: create a valid snapshot, change only `schema_version` from 1 to 2,
and send a normal RPC. The observed response is OK with score `0.014`, model
`demo-heuristic-v1`, feature version `0:1`, and `demo_model=true`. The score is
diagnostic output from a heuristic, not a model-quality measurement.

The correction should validate the retrieved schema before invoking either
model path and return `FAILED_PRECONDITION` for missing or unsupported schema.
Keep the runtime fix and regression changes together. Remove the `xfail` marker
from `test_incompatible_snapshot_schema_rejected_before_inference`, then extend
it to missing schema and a locally trained synthetic artifact. Preserve valid
nullable feature values used by the model. Do not change feature definitions or
retrain the real-data model to solve a serving validation problem.

Adjacent payload cases need explicit probes before a broader validator is
implemented: malformed JSON, missing timestamp/features/version, a non-finite
timestamp, mismatched entity/transaction IDs, and missing model columns. Do not
claim these are currently rejected with a defined status. For the follow-up
contract, invalid stored payloads should fail with `FAILED_PRECONDITION`, without
inference or payload details in the client error; store connectivity failures
remain `UNAVAILABLE`. Unexpected internal inference failures are a separate
server-error case and should not be relabeled as client input errors.

## Deadline limits and follow-up

The probe waits until the handler enters an injected blocking GET, allows the
real client deadline to expire, then releases the GET and observes handler
completion. This separates transport deadline behavior from cancellation of
work. No arbitrary sleep determines when the store call begins. The test has
bounded waits and releases its worker in cleanup.

The implementation checks `context.is_active()` only after fetching and checking
freshness. It does not pass the remaining deadline to Redis, check cancellation
before reading, or stop an in-progress prediction. `FeatureStore` configures
two-second socket and connection timeouts independently of an RPC deadline.
The new test establishes skipped inference after an expired read, not bounded
server resource consumption or cancellation during prediction. A later deadline
change needs blocked-read and blocked-inference probes plus a healthy request
after failure; client `DEADLINE_EXCEEDED` alone cannot prove worker recovery.

## Verification and integration handoff

Run from the checkout with its `src` selected in `PYTHONPATH`, following the
[reproducibility guide](REPRODUCIBILITY.md):

```text
python -m pytest -q tests/test_serving.py tests/test_scoring_contract.py --basetemp=.pytest_cache/scoring-contract -rx
python -m pytest -q tests/test_scoring_contract.py -k incompatible --runxfail --basetemp=.pytest_cache/scoring-reproducer
```

Observed: the first command passed 13 cases with 1 strict expected failure in
3.64 seconds. The second intentionally disabled expected-failure handling and
failed the one selected case in 0.76 seconds because the incompatible snapshot
received a successful score. The expected failure is an open acceptance item,
not a passed correctness check. An unexpected transport error remains a real
test failure, and a successful future rejection becomes a strict XPASS until
the marker is removed.

The full suite passed 206 tests with 1 expected failure in 13.28 seconds using
`python -m pytest -q --basetemp=.pytest_cache/scoring-full -rx`. Ruff, the
non-mutating protobuf check, and `pip check` also passed in the existing
development environment with imports confirmed inside this checkout.

For the next integration run, first establish Docker/Redis/Kafka availability;
the earlier local Docker tooling blocker has not been rechecked here. Use a
fresh entity prefix, send events through Kafka, wait within a bounded deadline
for Redis materialization, compare snapshot payloads with batch features, and
score via a loaded trained artifact. Record the actual model checksum/version,
snapshot schema, source feature version, age limit, response age, demo flag,
completed request count, and failures. Assert response feature version equals
the retrieved snapshot version and age falls within the configured freshness
limit. Use a stale event in a separate test namespace to verify rejection even
while its Redis key still exists. Verify a healthy score after any controlled
store outage. Do not disrupt unrelated running services.

`scripts/smoke.py` currently checks payload parity and score range, but does not
assert the returned demo flag, expected model version, feature version, or
freshness. Strengthen those checks with the integration deliverable before
claiming trained scoring. Synthetic artifact tests establish wiring only;
real-data evaluation and the pending validation-only model experiment remain
separate. No fresh full-stack, Kafka crash, capacity, or model-quality result is
claimed by this document.
