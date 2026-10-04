# Scoring and Redis measurement protocol

Protocol frozen 2026-09-17, before new performance measurements. This extends
the [benchmark guide](BENCHMARKS.md); it does not replace its sustained-run gate.
No new throughput or latency result is claimed here. The earlier 30-second
scoring and 10,000-GET results in [validation](../reports/VALIDATION.md) remain
smoke evidence from a shared host.

## Preconditions and scope

Measure reads of already materialized immutable snapshots, separately for gRPC
and Redis. Do not describe this as Kafka throughput or ingestion-to-decision
latency. Do not run crash campaigns, training, replay, or the two load generators
concurrently during isolated measurements. Run a combined streaming/scoring
experiment only later, with its own configuration and lag/freshness accounting.

Before load, pass unit tests and the trained full-stack smoke with exact model
identity, feature version, schema and freshness checks described in
[scoring failures](SCORING_FAILURES.md). The snapshot-schema rejection defect,
validation-only model experiment and interrupted recovery campaign remain open
at this protocol's starting revision. Diagnostic load can proceed with those
limitations recorded; it cannot close these correctness or release gates.

Use the same verified existing trained artifact for both sides of a comparison.
Record its metadata and model SHA-256, model version, source revision, dependency
versions and image digests. Set `ALLOW_DEMO_MODEL=0`. Do not retrain or select a
model based on load results. If no compatible trained artifact exists, stop the
trained measurement; an explicitly labeled heuristic smoke is a separate cell.

## Fixed workload

- Generate 10,000 transactions over 1,000 possible entities with seed 42, using
  the existing generator's amounts, 40 merchants and geographic distribution.
  Entity occupancy is random; record actual distinct entities rather than
  assuming all 1,000 appear. Transaction selection is uniform over rows, not
  uniform over entities. No special hot-key skew is introduced.
- Generate once immediately before the paired campaign, at
  `data/transactions.jsonl`; retain the exact bytes and SHA-256 for both versions.
  The CLI places its 100 seconds of event time in the recent past. Do not use
  the default historical timestamps of `synthetic()` directly for serving.
- Use one dedicated local namespace, `perf-local-v1`, consistently in producer,
  workers, materializer, scorer and host Redis reader. Populate it once through
  Kafka, then wait for zero consumer lag and verify every corpus snapshot.
  Never reuse conflicting IDs or delete an existing shared namespace to prepare
  a run. Select and record a fresh namespace if this one is already occupied.
- Preflight all 10,000 keys for matching IDs, schema, expected feature version,
  and payload; record hit rate and payload byte distribution. Require 100% hits
  for this controlled experiment, stricter than the script's 99% gate. Spot
  check gRPC output against the same snapshots and exact model identity.
- Keep TTL and maximum feature age at 86,400 seconds. Record the oldest event
  age and minimum remaining key TTL before each run; require enough margin for
  warm-up, measurement, drain and 60 seconds. Recheck after each run. If renewal
  becomes necessary, declare a new corpus/configuration and restart the pair.
- Redis readers already seed each worker with its index. Before paired k6 runs,
  add a recorded per-VU deterministic random seed (base 42) to replace unseeded
  `Math.random()`. Arrival scheduling can still change exact cross-VU ordering;
  retain that limitation instead of claiming identical request order.

Preparation commands from the worktree, with Python imports directed to its
`src`, are supported by the current CLI:

```powershell
python -m fraud_pipeline generate --count 10000 --entities 1000 --seed 42 --output data/transactions.jsonl
Get-FileHash data/transactions.jsonl -Algorithm SHA256
# After all services use the dedicated namespace:
python -m fraud_pipeline replay --input data/transactions.jsonl --rate 1000
```

Replay completion alone is not proof of materialization. Compose currently
hardcodes `fraud` in its shared environment: setting the host environment alone
does not change container namespaces. Prepare and inspect a Compose override
for the named services before running these commands against the stack.

## Topology and resource budget

Primary scoring topology: k6 container -> scorer container -> Redis container
on the same Docker network and shared Windows Docker Desktop host. Plaintext
gRPC, one scorer, 16 Python worker threads, maximum 256 concurrent RPCs, XGBoost
`n_jobs=1`; no replicas or load balancer. k6 uses 200 preallocated and at most
1,000 VUs, 5-second connect and 2-second RPC timeout. Keep these fixed across
rates, including overload; do not silently increase VUs to hide drops.

Primary Redis topology: original Python interpreter with worktree imports on
Windows -> published localhost port 6379 -> Redis container. Label the Docker
Desktop boundary explicitly. Do not compare it as equivalent to in-container
GET latency. Readers share the script's connection pool; each reader has at
most one GET outstanding, with 2-second socket/connect timeouts.

The following are **planned enforced limits**, not current Compose settings:

| Process/container | CPU limit | Memory limit |
| --- | --- | --- |
| Scorer | 2 CPUs | 2 GiB |
| Redis | 1 CPU | 1 GiB; existing 512 MiB maxmemory, noeviction, AOF on |
| k6 | 2 CPUs | 1 GiB |
| Kafka during setup | 2 CPUs | 2 GiB |
| Each of four workers; materializer | 0.5 CPU each | 512 MiB each |

Implement a local Compose override before measurement and inspect effective
limits with `docker compose config` and `docker inspect`. After corpus parity
and lag checks, stop the campaign's producers/workers/materializer/Kafka for the
isolated read phase; leave Redis/scorer running. Do not stop unrelated services.
Use the same Redis AOF and persistence settings before/after. The native Redis
client has no enforced CPU/memory cap: concurrency is its workload bound, and
client CPU/RSS must be recorded. A future containerized reader is a new topology.

Require a recorded host with at least 8 logical CPUs and 16 GiB RAM, Docker
allocation at least 6 CPUs/8 GiB, and at least 10 GiB free disk before setup.
These are campaign admission limits, not observed hardware facts. If unavailable,
freeze a smaller budget before any run and label results accordingly. Do not
change host permissions or provision infrastructure to satisfy this protocol.
Abort and preserve evidence on OOM/container restart, disk below 5 GiB or host
available memory below 2 GiB; do not keep extending an unstable run.

## Run matrix and ordering

Run one cell at a time in ascending order. Warm-ups use the same target load
as their measured cell, in a separate output file, with counters reset afterward.
Allow 30 seconds idle between cells. Start resource collection before warm-up.

| ID | Offered workload | Warm-up | Recorded run | Purpose |
| --- | --- | --- | --- | --- |
| S-smoke | 100 scoring arrivals/s | 30 s | 30 s once | Wiring only |
| S-screen | 100, 500, 1,000, 2,500, 5,000 arrivals/s | 60 s each | 60 s each | Find overload/profile candidate |
| S-steady | 100 arrivals/s primary; highest passing screen rate secondary | 120 s each | Three independent 900 s runs per rate/version | Sustained serving evidence |
| R-smoke | 16 readers, 10,000 total GETs | 1,600 GETs | One fixed-count run | Wiring only |
| R-screen | 1, 4, 16, 32 readers | 60 s each | 60 s each | Concurrency response |
| R-steady | 16 readers primary; highest passing screen concurrency secondary | 120 s each | Three independent 900 s runs per concurrency/version | Sustained store evidence |

If the primary and secondary cell coincide, do not duplicate it. A screen passes
only with all validity gates and the latency/error thresholds below; stop
escalating after the first failing screen. If even the first cell fails, retain
that result and diagnose it. A screen does not establish capacity.

Prioritize baseline screens, profiling, then the paired primary cells. Run the
same baseline and candidate cells in paired order B1/C1, C2/B2, B3/C3, with warm-up
each time. Use separate processes/directories for the two source revisions,
without switching or resetting this worktree's branch. Reuse the corpus, model,
limits and namespace. Record service restarts. If time cannot accommodate the
complete matrix, publish completed cells and mark the others not run; do not
shorten the sustained protocol or substitute an extrapolation.

## Accounting and instrumentation gate

Current scripts support smoke checks, but need the following instrumentation
before the sustained comparison. Keep each change with focused regression tests.

| Current behavior | Required measurement behavior |
| --- | --- |
| Redis executes `concurrency * floor(reads / concurrency)` | Execute exact read budgets; separately count requested, attempted, hit, miss and error |
| Redis connection errors escape before a report is written | Preserve partial counts, error types, elapsed time and failed-run status |
| Redis reports elapsed time for a fixed count only | Add monotonic duration mode with bounded drain and stop reason; keep fixed-count mode |
| Redis percentiles combine hits and misses | Retain all-attempt latency and successful-hit latency separately; no silent failure exclusion |
| k6 checks status and an `xgb-` prefix | Require exact expected model version and `demo_model=false`, finite risk in [0,1], valid feature version and age |
| k6 catches errors without category or end-to-end timing | Count connection, transport/status and response-contract failures; time full iterations as well as RPCs |
| Neither script records resource use | Collect time-aligned CPU/RSS/container memory, throttling, restarts and host load separately |

Preserve existing k6 RPC p50/median, p95 and p99, `successful_scores`,
`scoring_errors`, `dropped_iterations`, iterations and duration. Report scheduled
arrivals, started iterations, successes, failures, interrupted/in-flight work and
drops so totals reconcile; failed connection attempts are not successful RPCs.
Use the fixed 900-second arrival window for achieved arrival-window RPS and
record the drain interval separately. Also report total completions divided by
actual elapsed time, explicitly naming the denominator. Do not present the
k6 counter's rate as identical without checking its measurement interval.

Redis is closed-loop: dropped arrivals are **not applicable**, not zero. Report
successful GETs/actual measurement seconds, misses and exceptions; connection
setup is included in client GET timing when it occurs. Stop scheduling at the
duration boundary, drain outstanding GETs within the configured timeout, and
record drain time and any unfinished calls. Reconcile
`attempted = hits + misses + errors + unfinished`.
Bounded samples/histograms must retain percentile precision
metadata rather than growing an unbounded list during long runs.

For a valid scoring cell require zero drops, error fraction <0.001, RPC p99
<100 ms, exact trained identity, and no freshness/schema violations. The existing
success threshold of offered rate *0.999 is weaker than a literal 5,000 successful
RPS claim: report actual successful RPS and mark the target missed when below it.
For this Redis corpus require zero misses/errors and report whether successful
GET p99 <1 ms; exceeding 1 ms is a failed latency target, not discarded evidence.
Keep invalid runs and reason codes. Never average percentiles into a pooled p99.

Collect resources every second during all measured cells, including the native
client and k6, not just the server. Retain sampling cadence, missing samples,
CPU normalization, RSS/container peaks, Redis used memory/evictions and scorer
RPC metrics. If sampling is unavailable, state the gap and make no attribution
of a bottleneck to CPU or memory. Profile a separate matching diagnostic run;
do not compare profiled baseline timing to unprofiled candidate timing.

## Evidence and handoff

Use unique ignored `reports/*.json`/`*.csv` outputs for run IDs and retain them
locally. Commit only a reviewed aggregate Markdown report with commands, config,
hashes, individual run percentiles/RPS/errors/drops/resource summaries and limits.
Do not commit raw logs, corpus files, environments or model artifacts. Document
the proposed bottleneck with profile evidence before optimizing; if none is
justified, improve accounting and say no runtime optimization was established.

On 2026-09-17 the new `tests/test_benchmark_accounting.py` probes execute the
actual Redis CLI with injected GET responses. They verify the inclusive 99% hit
gate and report retention for low hit rates; strict expected failures expose
17 requested reads becoming 16 at concurrency 4 and absence of a report on a
connection exception. These are deterministic accounting diagnostics, not real
Redis measurements. Remove each marker with its eventual implementation fix.

Local preflight found neither Docker nor k6 on PATH. Windows CIM hardware queries
were denied, so hardware admission and container resource availability remain
unverified. No service startup, profiling or load test was performed for this
protocol. Resolve tools/resource visibility through the normal environment before
running the matrix; record blocked cells honestly if still unavailable.
