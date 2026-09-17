# Benchmark guide

For a fixed workload, resource budget, run matrix and accounting prerequisites,
follow the [scoring and Redis measurement protocol](PERFORMANCE_TEST_PLAN.md).
Its sustained runs require instrumentation beyond the current smoke scripts.

All commands below assume the project's virtual environment is active and the local Docker stack is healthy. In PowerShell it can be activated with `.\.venv\Scripts\Activate.ps1`, or invoke executables through `.venv\Scripts` as shown in the README.

## Correctness first

```powershell
python -m pytest -q
python scripts/smoke.py
python -m scripts.crash_trials --trials 100 --output reports/crash-trials.json
```

The crash harness uses isolated topic namespaces and real worker subprocesses that exit abruptly before or after a Kafka transaction commit, while windows are active. It injects duplicate inputs, restarts the worker, reads only committed outputs, compares all 12 features against an independent reference, verifies restored state and offsets, and reapplies Redis snapshots twice. Seeds, injection points, failures, and logs are recorded.

The harness deletes only its successfully checked temporary topics. Failed topics remain for investigation. Redis trial keys expire after five minutes. Worker `os._exit(137)` intentionally bypasses cleanup. These are actual worker process crashes, but not broker failures, network partitions, dynamic rebalances, or materializer process-kill tests. The 100 checkpoint unit cases are separately labeled simulations and must not be counted as these trials.

Start with `--trials 2` before a long run. Do not run the harness against production brokers or reuse application topic names.

## Streaming capacity

```powershell
fraud generate --count 100000 --entities 10000
fraud replay --input data/transactions.jsonl --rate 5000
```

The replay command reports **producer delivery throughput**, not completed processing throughput. Inspect committed worker event counters and Kafka consumer lag:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group fraud.features.v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group fraud.redis.v1
```

For larger tests, use a long enough input trace and raise `--rate` gradually. At 50k events/sec, 15 minutes requires 45 million input events; plan disk, Kafka retention, Redis capacity, and input generation accordingly. A short replay or repeated already-seen IDs is not proof of sustained useful processing. The generator defaults to 100 simulated events/sec of event time; large offline traces therefore span historical time and are for stream capacity, not necessarily fresh scoring snapshots.

Record accepted, rejected, and duplicate rates separately. The materializer uses individual synchronous writes and commits in this first version, so expect to profile it. Redis is configured with `noeviction`; reaching its memory budget is a visible failure rather than silent feature loss.

## gRPC load

The optional Compose service runs k6 without a native install:

```powershell
docker compose --profile benchmarks run --rm k6
```

Its default is a trained-model check at 100 RPS for 30 seconds. Set exported `RATE` and `DURATION` to override those values. It connects to the scorer over the Docker network. The commands below are the equivalent native-k6 workflow.

Generate a fresh scoring corpus, replay it, and verify all its snapshots exist before measuring. Keep the same corpus at `data/transactions.jsonl`, which the k6 script reads. Use a separate namespace for clean corpus histories if needed.

```powershell
$env:RATE = '100'
$env:DURATION = '30s'
$env:REQUIRE_TRAINED = '0'
k6 run --summary-export reports/k6-smoke.json benchmarks/scoring.js

# Train and deploy the XGBoost artifact before the measured run below.
$env:RATE = '5000'
$env:DURATION = '15m'
$env:REQUIRE_TRAINED = '1'
k6 run --summary-export reports/k6-scoring.json benchmarks/scoring.js
```

The constant-arrival-rate workload reports RPC latency, errors, successful score rate, and dropped iterations. Thresholds fail when p99 reaches 100 ms, errors reach 0.1%, or any iteration is dropped. The successful-rate threshold allows the stated 0.1% error budget; separately check the report against any claim of 5,000 successful RPS. A warm-up run should precede recorded runs. Record whether the model was the demo heuristic or trained XGBoost; only the latter supports a model-serving claim.

The script samples many immutable transaction snapshots rather than requesting the same key exclusively. It measures serving of already materialized features; it does not measure Kafka ingestion-to-decision latency.

## Redis reads

```powershell
python benchmarks/redis_reads.py --reads 100000 --concurrency 16 --output reports/redis.json
```

The script measures client-observed GET latency in milliseconds, hit rate, payload size, and elapsed time. It fails when fewer than 99% of keys are present. Record CPU/network placement and distinguish localhost from cross-host results. This closed-loop microbenchmark is separate from the k6 offered-load experiment; adjust its read count to cover a useful duration.

## Report every configuration

Use at least a warm-up and three 15-minute steady runs for final throughput claims. Retain the individual reports; do not average p99s and present the result as a combined p99. Record:

- Commit or source archive hash, package versions, image versions, seed, and full commands.
- Host OS, CPU, RAM, disk, network placement, and load-generator host.
- Kafka partitions, batch size, message sizes, entity cardinality, and hot-key distribution.
- Model version, feature retention, Redis memory, hit rate, and errors.
- Offered, delivered, and successfully processed rates; lag before/during/after load.
- Latency percentiles, recovery time, CPU/memory peaks, and failures.

Finally run streaming and scoring together. This repository does not yet automatically measure complete ingestion-to-Redis freshness or export consumer lag into Grafana; use the Kafka command above and add timestamp instrumentation before claiming an end-to-end freshness SLA.
