# Event-time correctness validation

Verified locally on 2026-09-13 using the existing Python 3.12.8 development
environment, with imports explicitly directed to the active checkout's `src`.
The added cases pass against the existing runtime implementation. No production
defect was reproduced or runtime correction made.

## Boundary coverage

`tests/test_event_time.py` adds 15 hand-worked cases independent of batch output:

| Contract | Observed behavior |
| --- | --- |
| 60, 300, and 3600-second windows | Include the exact lower endpoint and its next representable float; exclude the preceding float and equal-time peers |
| Equal timestamps | Peers have no prior-time features, remain in history, and the last peer in input order supplies the next event's previous location |
| Duplicate age | Ages 86399 and 86400 seconds reject; 86401 accepts with empty prior history |
| Duplicate payload | Changed amount still rejects; rejection does not refresh ID retention |
| Partition lateness | Exactly 3600 seconds behind accepts for a new entity; 0.001 seconds further behind rejects |
| Entity isolation | The same transaction ID on another entity accepts with empty history |
| Rejection precedence | Partition lateness precedes duplicate detection, which precedes entity lateness; all rejections preserve state and watermark |
| Entity expiry | Exactly 86400 seconds retains state; the next representable watermark expires it once, including after JSON restoration |

Expiry coverage also checks that an obsolete heap entry cannot remove refreshed
entity state and equal-time entries cannot generate duplicate tombstones.

## Partitioned parity trace

`test_edge_trace_partitioned_parity_and_checkpoint_cuts` uses 13 input records
and 11 explicitly selected accepted records. The input includes three entities,
an ID shared across entities, equal-time peers, distinct merchants, exact minute,
five-minute and hour boundaries, a history gap, a changed-payload duplicate,
a duplicate exactly at 24 hours, and ID reuse one second beyond retention.

The trace runs in one shared partition, two partitions, and three separate
partitions. Each layout is restored through JSON at every one of the 14 cuts
(before input, between records, and after input), for 42 cases. Expiry runs at
simulated three-record batch boundaries and before checkpoint restoration.
Every status and accepted snapshot identity is asserted. All 12 feature values
are compared with the independent brute-force oracle in `tests/test_features.py`,
then complete in-process stream snapshots are compared with batch snapshots.
The oracle receives hand-selected accepted inputs rather than engine-filtered
inputs, so agreement cannot conceal dropped or wrongly accepted records.

A separate case shows that a cross-entity timestamp regression can be accepted
by a shared stream engine but must be rejected by batch's global sorting rule.
This explains the scope of parity rather than promising arbitrary replay parity.

These are in-process tests. They do not exercise Kafka partition assignment,
transactions, process termination, state-topic replay, Redis conflicts on ID
reuse, or end-to-end scoring. ID reuse beyond retention remains unsupported in
the same live snapshot namespace despite being accepted by the feature engine.

## Reproduction and results

Use the interpreter setup in [REPRODUCIBILITY.md](../docs/REPRODUCIBILITY.md),
including `PYTHONPATH` and `PYTHONDONTWRITEBYTECODE` for a shared virtualenv.

| Command | Result |
| --- | --- |
| `python -m pytest -q --basetemp=.pytest_cache/event-time-baseline` before changes | 138 passed in 15.75 seconds |
| `python -m pytest -q tests/test_event_time.py tests/test_features.py --basetemp=.pytest_cache/event-time-focused` after boundary coverage | 136 passed in 1.23 seconds |
| `python -m pytest -q tests/test_features.py tests/test_event_time.py --basetemp=.pytest_cache/parity-focused` after parity coverage | 179 passed in 1.22 seconds |
| `python -m ruff check src tests scripts benchmarks --exclude src/fraud_pipeline/generated` | Passed after both test additions |
| `python -m pytest -q --basetemp=.pytest_cache/event-time-full` | 196 passed in 7.01 seconds |
| `python scripts/generate_proto.py --check` | Bindings match the contract |
| `python -m pip check` | No broken requirements found |

No fresh infrastructure, dataset training, performance, or remote CI result is
claimed by this report.
