# Local validation

Validation date: 2026-09-12. Environment: Windows, Python 3.12.8.

## Checked locally

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

The initial real-data model does not meet the 0.90 ROC-AUC target. Sustained 50k transaction/sec and 5k scoring RPS benchmarks have not been performed. AWS deployment is deferred at the user's request. CI is configured but has not been triggered by a push.

Passing checkpoint simulations is not equivalent to passing Kafka process-kill trials; those have separate machine-readable evidence.
