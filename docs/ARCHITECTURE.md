# Architecture and consistency contract

## Event contract

`Transaction` validates a stable transaction ID, entity ID, nonnegative finite Unix timestamp and amount, merchant ID, and optional valid latitude/longitude pair. Amounts in live events must already be normalized to USD. The IEEE-CIS adapter uses dataset amount units under that schema convention; it does not infer an exchange rate.

Kafka keys must equal the UTF-8 entity ID. All producers must use the same partitioner and partition count so an entity always reaches the same partition. The supplied producer uses librdkafka's default partitioner. Do not increase a live namespace's partition count or switch partitioners without rebuilding its state in a new namespace.

IDs are unique within an entity, with a 24-hour event-time deduplication horizon. Reusing an ID with a different payload inside that horizon is treated as a duplicate input; producers are responsible for immutable IDs. Redis detects a conflicting immutable snapshot if one is produced later. Replays outside the retention horizon require a separate namespace.

The deduplication cutoff is inclusive: an ID accepted at `t` is still a duplicate
at `t + 86400`. A rejected duplicate does not refresh its retention timestamp or
advance the watermark. Beyond that horizon the feature engine can accept the ID
again; this is not permission to reuse an immutable Redis snapshot key in the
same namespace.

## Event-time policy

The worker accepts nondecreasing timestamps within an entity, including equal timestamps. It rejects events older than that entity's last accepted timestamp, and events more than one hour behind its partition's maximum accepted timestamp. There is no out-of-order buffer or correction stream in this version.

Exactly one hour behind the partition watermark is allowed if the entity's own
ordering permits it. Classification checks partition lateness first, then
duplicate ID, then entity lateness. Thus a known ID behind its entity timestamp
can be classified as duplicate, but an input beyond the partition lateness limit
is classified as late even if its ID is known. Neither rejection mutates feature
state, deduplication state, the expiry index, or the watermark.

Windows are `[timestamp-window, timestamp)`. Equal-time events are retained but excluded from one another's features. For subsequent events, the last preceding event in input order is the previous transaction. Previous-event and geographic features are missing when no earlier event exists in the one-hour retained history; they do not search arbitrarily far into the past.

Batch inputs must be globally sorted by timestamp. This guarantees that the batch's single engine and the live engines agree despite their different partition layouts. Production producers must enforce per-entity ordering; arbitrary out-of-order replay is not parity-equivalent.

Inactive entity state expires when the partition event-time watermark advances more than 24 hours beyond its last event. With no new input, state does not expire by wall clock. There is no absolute count limit on histories or deduplication entries inside the horizon; high event rates and cardinality can still exhaust memory.

See [event-time validation](../reports/EVENT_TIME.md) for independently checked
cutoffs, the partitioned parity trace, and checkpoint simulation limits.

## Kafka recovery

Each worker manually assigns one partition. Its stable transactional producer ID fences an earlier process with the same ownership. This is static partition assignment, not consumer-group rebalance management. Only the supplied static workers should use the feature group.

At startup the worker:

1. Initializes its transactional producer, resolving/fencing an old owner's transactions.
2. Scans its compacted state partition using `read_committed` until EOF.
3. Reconstructs entity snapshots, expiry indexing, the event-time watermark, and checkpointed next input offset.
4. Checks that the checkpoint offset equals the consumer group's committed offset and that the source log still retains that position.
5. Assigns the input partition at the recovered offset.

For each batch it computes features, produces output snapshots, writes touched entity checkpoints and expiration tombstones, and sends next input offsets to the **same Kafka transaction**. It commits once. On a transaction error it exits instead of continuing with mutated local state. A restart always recovers from committed Kafka state, including when the prior commit outcome was ambiguous.

This follows the [Confluent Python transactional API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html): send consumed offsets plus one into the transaction, and use committed isolation downstream.

The state topic uses compaction without time-based deletion. Transaction/output/rejected topics retain seven days in the supplied initializer. Group offsets must remain available; if offsets are lost but a checkpoint survives, recovery fails closed. A 120-second restoration timeout is deliberate and configurable in code; large state may require increasing it after profiling.

Full touched-entity snapshots are easy to inspect but expensive. Records are capped at 10 MB; a hot entity can exceed this and stop the worker. Incremental changelogs and optimized rolling aggregates are the next scaling work, not capabilities this version claims to have.

## Redis and scoring

The materializer consumes committed feature records. An atomic Lua script stores an immutable JSON payload keyed by a hash of `(entity_id, transaction_id)` in a versioned namespace. Reapplying the identical value is safe and refreshes its TTL; a different value for the same key fails. Only after a successful Redis write does the consumer commit its position.

There is no global Kafka/Redis transaction. Redis can lag or temporarily be unavailable, and materialization can be replayed. If Redis is flushed, merely restarting the materializer is insufficient because its Kafka offsets may already be committed; reset its group as described in the runbook.

The gRPC API scores an existing transaction snapshot. It neither ingests new events nor reads a mutable "latest entity" feature vector. Clients submit the transaction to Kafka first and wait/retry until the snapshot is available. `feature_age_seconds` is event age, not a measurement of consumer lag.

The service returns `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION`, or `UNAVAILABLE` for defined failure cases. It bounds concurrency and checks feature age and request cancellation. The model artifact includes schema order and checksum validation. No model means startup failure unless the demo heuristic is explicitly enabled.

## Deployment boundary

Compose is a single-broker local demonstration with replication factor one, durable Docker volumes, plaintext traffic inside its Docker network, and localhost host ports. It does not survive loss of all broker storage. The gRPC server supports certificate/key environment variables for TLS; the sample CLI and k6 workload use plaintext over localhost/private tunnels.

The EC2 template deploys the same single-host topology. Broker redundancy, authenticated Kafka, online schema migration, dynamic rebalancing, calibrated probabilities, automatic model retraining, and automatic backfill/live cutover remain outside the implemented scope. Do not expose this demo stack publicly.
