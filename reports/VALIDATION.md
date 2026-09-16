# Local validation

Validation date: 2026-09-12. Environment: Windows, Python 3.12.8.

## Scoring failure probes, 2026-09-16

The [scoring failure contract](../docs/SCORING_FAILURES.md) reviews existing
coverage and records real localhost gRPC probes with injected stores. Freshness
boundaries, Redis connection/timeout error mapping, and skipped inference after
a deadline expires during a store read pass. An incompatible snapshot schema
is currently scored successfully: a strict expected-failure test preserves this
open defect, and running it with `--runxfail` reproduces the failed rejection.
This is demo-model transport evidence, not live Redis or trained full-stack
verification. The document specifies the correction and integration acceptance
checks without claiming those remaining gates passed.

## Model experiment protocol, 2026-09-15

The [validation-only experiment protocol](../docs/MODEL_EXPERIMENT.md) freezes
one training-derived class-weight candidate, chronological partitions, feature
availability, leakage checks, and an average-precision selection rule before
training changes. A fresh read-only dataset audit matches the historical hash
and split counts. The previous test period has already been inspected; the
supplied competition test CSV has no labels. No new model-quality result is
claimed. Adapter leakage coverage is separate from the validation-only training
path, which remains to be implemented.

## Event-time checks, 2026-09-13

Fresh local verification passed **196 tests** in 7.01 seconds, Ruff, the
non-mutating protobuf check, and `pip check`. Added 15 independent boundary cases,
42 partition/checkpoint parity cases, and one batch ordering case. The existing
runtime passed; no runtime defect or correction is claimed. See
[event-time evidence](EVENT_TIME.md) for the trace, commands, and limits.
These checks used the existing development environment with worktree imports;
Kafka recovery and external-service integration were not rerun.

## Baseline commit verification

Rechecked on 2026-09-12 in a separate Git worktree before committing the existing implementation:

- `python -m pytest -q`: **129 passed** in 13.16 seconds.
- `python -m ruff check src tests scripts benchmarks --exclude src/fraud_pipeline/generated`: passed.
- `python -m pip check`: no broken requirements found in the existing development environment.
- `python -m fraud_pipeline demo`: processed 1,000 synthetic transactions and emitted 12 features using the explicitly untrained heuristic.

These commands used the existing development virtualenv with imports directed to the worktree's `src` directory. This was not a fresh dependency installation. Docker integration, training, crash trials, and performance benchmarks were not rerun for this commit; their previously recorded results appear below.

## Checked locally

The following sections preserve earlier implementation and integration evidence.
Fresh evening checks are recorded separately below; the historical integration,
training, recovery, and performance measurements were not rerun in that check.

- Feature windows and missing-value behavior against hand-worked cases and an independent brute-force oracle.
- Batch/stream parity on deterministic synthetic traces.
- Duplicate handling, invalid input, event-time rejection, and state expiration.
- 100 seeded checkpoint serialization/recovery simulations that discard uncommitted mutations.
- Real localhost gRPC requests, including successful scoring and invalid, missing, stale, and unavailable-store outcomes.
- XGBoost training on a small manufactured CSV, chronological split metadata, UBJSON save/load, and feature-schema rejection.
- Real IEEE-CIS training on 590,540 rows from `../ieee-fraud-detection/train_transaction.csv`: held-out ROC-AUC **0.672305** on 88,081 rows. See [the full baseline report](IEEE_BASELINE.md).
- Real Kafka worker crash campaign: **65 passed trials**, then trial 66 failed because the host-to-Kafka connection at `localhost:19092` became unavailable. The failed trial is retained with its log; this is not evidence for a 100/100 pass.
- Lint and generated protobuf bindings.

Result: **129 tests passed**. Ruff checks passed, and `pip check` found no broken requirements. The in-process demo processed **1,000 synthetic transactions**, emitted all **12 features**, and returned an explicitly labeled heuristic score. Five YAML configuration files and the Grafana dashboard JSON parsed successfully; this is syntax validation, not a Docker Compose runtime check.

The manufactured training fixture is a code test, not IEEE-CIS evaluation evidence.

## Docker integration verified locally

Docker Desktop was installed for the Windows user with the WSL 2 backend. Engine 29.7.2 and Compose v5.5.1 were verified. The application image built successfully, and the Kafka, Redis, workers, materializer, trained scorer, Prometheus, and Grafana containers started.

The full-stack smoke test passed for 20 transactions: Kafka ingestion, Redis snapshots, batch/stream feature equality, and gRPC scoring with model `xgb-06044eadd449` and `demo_model: false`. All five Prometheus scrape targets are up, and Grafana provisioned the four-panel Fraud Pipeline dashboard.

Local endpoints are Grafana `http://localhost:3001`, Prometheus `http://localhost:9090`, gRPC `localhost:50051`, and scorer metrics `http://localhost:8001/metrics`. Existing applications on ports 3000 and 8000 were left in place.

A first 30-second k6 smoke run completed 3,001 successful requests at 100.024 RPS, with no scoring errors or dropped iterations. This was a short check on the shared development host, not a 5,000-RPS capacity benchmark. The raw report is `reports/k6-local-smoke.json`.

A second 30-second run validated the new Compose k6 service and explicit percentile export: 3,001 successful scores at 100.018 RPS, no errors or dropped iterations, and RPC p99 **3.971386 ms**. The raw report is `reports/k6-scoring.json`. Both runs used the trained model and overlapped the separate crash campaign on the same development machine; neither is an isolated sustained-capacity benchmark.

A Windows-to-Docker Redis test completed 10,000 GETs at concurrency 16 with a 100% hit rate. Client-observed latency was p50 2.9392 ms, p95 5.6917 ms, and p99 7.4576 ms, missing the sub-millisecond target. The raw report is `reports/redis-local.json`.

## Remaining validation

The initial real-data model does not meet the 0.90 ROC-AUC target. Sustained 50k transaction/sec and 5k scoring RPS benchmarks have not been performed. AWS deployment is deferred at the user's request. Baseline CI has now been observed passing, as recorded below. Subsequent local changes have not been pushed and have no remote CI result.

Passing checkpoint simulations is not equivalent to passing Kafka process-kill trials; those have separate machine-readable evidence.

## Evening reproducibility check, 2026-09-12

The pre-existing baseline `90fe9fa` was rechecked using the existing Python 3.12.8
development environment with `PYTHONPATH` directed to the active checkout's `src`.
The import path was printed and confirmed before running checks.

| Command | Observed result |
| --- | --- |
| `python -m pytest -q` before changes | 129 passed in 12.64 seconds |
| `python -m ruff check src tests scripts benchmarks --exclude src/fraud_pipeline/generated` | Passed before and after changes |
| `python -m pip check` | No broken requirements before and after changes |
| `python -m fraud_pipeline demo` | 1,000 transactions, 12 features, untrained heuristic score 0.083986 |
| `python scripts/generate_proto.py` followed by `git diff --exit-code -- src/fraud_pipeline/generated` | No tracked binding drift |
| `python -m pytest -q tests/test_proto_generation.py --basetemp=.pytest_cache/proto-check` | 9 passed in 0.47 seconds |
| `python -m pytest -q --basetemp=.pytest_cache/evening-full` | 138 passed in 5.61 seconds |
| `python scripts/generate_proto.py --check` | Bindings match; no rewrite |

Change `c430684` adds a non-mutating protobuf check and switches CI to use it.
The nine new cases use the real pinned compiler to verify matching bindings,
each missing/stale binding, Windows line endings, contract drift followed by
regeneration, invalid compiler input, and CLI invocation outside the repository.
This closes a setup verification gap; no runtime fraud-pipeline defect was found
or claimed in this work.

Read-only inspection with `gh run view 34679816385 --json headSha,conclusion,url,jobs`
confirmed that [baseline CI run 34679816385](https://github.com/ShreyashDhakate/Real-Time-Fraud-Detection/actions/runs/34679816385)
completed successfully for `90fe9faed43993af67ac9513c5d67f0a33355cc6`.
Its job reported successful dependency installation, protobuf generation and
drift check, Ruff, pytest, demo, Compose config/build/start, full-stack smoke,
and the two-trial process-kill command. These are observed remote step results
for the baseline, not new local runs or a 100-trial recovery campaign.
The new protobuf check has only been verified locally.

`docker compose config --quiet` and `docker info` could not run locally because
`docker` was not available on the session's PATH; checks of common executable
locations did not locate it. This does not invalidate the earlier Docker results
or establish the current engine state.

This was not a clean dependency installation. No fresh Docker integration,
real-data training, crash campaign, or performance measurement is claimed.
Follow [the reproducibility guide](../docs/REPRODUCIBILITY.md) for commands and
the evidence required to close each remaining validation gate.
