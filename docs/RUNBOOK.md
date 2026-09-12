# Operations runbook

## Start, inspect, stop

```powershell
docker compose up --build -d
docker compose ps
docker compose logs --tail 100 worker-0 worker-1 worker-2 worker-3 materializer scorer
python scripts/smoke.py
docker compose down
```

The smoke test uses unique entity IDs so it does not change the expected history of the demo corpus. It can be rerun. Generate a fresh corpus for separate benchmark runs; do not replay an old input into entities whose event time has already advanced and expect it to be accepted.

Docker volumes retain Kafka and Redis data after `down`. Removing volumes permanently discards the development event log, group positions, state checkpoints, and Redis projection. Use `docker compose down --volumes` only for an intentional complete local reset, after saving any required reports or data.

The worker and scorer expose Prometheus metrics. Enable `--profile monitoring` to start the included Grafana dashboard. Its RPC p99 is server-side timing; use k6 for client-observed latency. Kafka lag is currently inspected through `kafka-consumer-groups.sh`, not exported by the included dashboard.

## Recover a worker

```powershell
docker compose restart worker-0
docker compose logs --tail 100 worker-0
```

Recovery fences the old transactional producer and restores the compacted checkpoint log. Input offsets and checkpoint offsets must agree. Do not delete the checkpoint topic or reset only the feature consumer group's offsets: that invalidates its state relationship.

If recovery reports expired input offsets or a missing committed group position, stop the affected worker and investigate retention. Recover from an archived complete event history into a **new namespace**; this version deliberately refuses to guess at a safe offset. There is no implemented automatic archive restore.

If one entity's state exceeds the Kafka message limit, lowering batch size does not solve that entity's snapshot size. Profile and implement incremental changelogs or change the supported rate/retention contract before proceeding.

## Recover Redis after data loss

Ordinary materializer restarts replay only uncommitted input. To rebuild after Redis data loss, stop the materializer and reset its own consumer group to the beginning of the retained feature topic:

```powershell
docker compose stop materializer
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group fraud.redis.v1 --topic fraud.features --reset-offsets --to-earliest --execute
docker compose up -d materializer
```

This reset targets the Redis projection group only, **not** `fraud.features.v1`, the stateful processing group. Scoring may return `NOT_FOUND` until replay catches up. Features older than Kafka retention cannot be recovered by this method. Replaying historical features refreshes Redis TTL, but the scoring service still rejects old event timestamps.

Conflicting immutable values indicate namespace reuse, duplicate ID misuse, or an incompatible backfill. Inspect the conflicting event and key; do not overwrite it blindly. Out-of-memory Redis errors are visible because its policy is `noeviction`; budget memory for the number and size of retained transaction snapshots.

## Batch features and backfill

```powershell
fraud batch --input data/transactions.jsonl --output data/features.jsonl
fraud backfill --input data/features.jsonl --namespace fraud-backfill-v1
```

Batch input must be globally timestamp sorted. It uses the same definitions as streaming, with the current event excluded. The backfill CLI rejects the live namespace and writes immutable per-transaction snapshots under a separate namespace. Rerunning identical offline snapshots is idempotent.

This is an offline backfill tool, not an automatic live migration. Offline versions are prefixed `offline:` and should not be mixed with Kafka versions in the same namespace. Validate feature payloads against a live replay before selecting a new namespace. A production catch-up and atomic live cutover protocol remains future work; do not switch the scorer to a partially backfilled namespace.

## Model rollout

1. Train to a new directory, for example `artifacts/candidate`, and inspect its evaluation and metadata.
2. Validate the candidate with representative requests and compare its schema and intended data domain to live events.
3. Preserve the currently deployed directory as a rollback artifact. Stop the scorer before replacing the complete `artifacts/model` bundle, or point `MODEL_DIR` at a versioned directory and recreate the container.
4. Set `ALLOW_DEMO_MODEL=0` in `.env`; recreate the scorer and run the smoke test.
5. Check the returned model version. Roll back by restoring the previous directory/configuration and recreating the scorer.

The service verifies the model file checksum and feature order at startup. Models are stored as XGBoost UBJSON rather than unpickling arbitrary Python objects. See [XGBoost model serialization](https://xgboost.readthedocs.io/en/stable/tutorials/saving_model.html).

Training uses an IEEE-CIS entity proxy while the synthetic demo uses explicit synthetic accounts. Synthetic scores demonstrate wiring; they are not evidence that the trained model generalizes to a new payment population.

## EC2 deployment

Complete the AWS prerequisites in [EXTERNAL_SETUP.md](EXTERNAL_SETUP.md). From `infra/terraform`:

```text
terraform init
terraform validate
terraform plan
terraform apply
```

The template expects an existing private subnet with working outbound access. It installs Docker and Compose on Ubuntu and creates `/opt/fraud`. Check cloud-init logs and `docker compose version` through Systems Manager before deploying. SSM Agent availability depends on the selected AMI; verify it if the instance does not appear as a managed node.

Transfer the source and trained artifacts through an organization-approved private Git repository or controlled artifact channel. No repository URL or bucket is hardcoded. If using a private repository, use a scoped credential mechanism; do not embed tokens in shell history or user data. Put the source under `/opt/fraud`, ensure the intended operator can read it, and run Docker commands with `sudo` unless that operator has explicitly been granted Docker access.

On EC2:

```bash
cd /opt/fraud
cp .env.example .env
# Edit .env and set ALLOW_DEMO_MODEL=0 after placing a model in artifacts/model.
sudo docker compose up --build -d
sudo docker compose ps
```

The default application runs as a non-root container user. Host bind-mounted `data` must be writable by that user for in-container generation; model artifacts only need read access. Generate on the host or set appropriate ownership rather than opening permissions broadly.

Use [SSM port forwarding](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-sessions-start.html) to reach localhost-bound ports. Example AWS CLI parameters, supplied as a JSON file to avoid shell quoting differences:

```json
{"portNumber":["50051"],"localPortNumber":["50051"]}
```

Save that as a temporary `ssm-ports.json`, then run:

```text
aws ssm start-session --target YOUR_INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters file://ssm-ports.json
```

The SSM tunnel protects remote transport. The sample scorer and load client communicate plaintext inside that tunnel. The server can use `GRPC_TLS_CERT` and `GRPC_TLS_KEY` when a deployment provides certificates, but authentication and multi-host Kafka/Redis transport configuration require additional deployment work.

## Teardown and costs

Save benchmark evidence and required model artifacts first. `terraform destroy` terminates the instance and removes managed IAM/security resources. The root EBS volume deliberately has `delete_on_termination=false`: inspect and explicitly remove or snapshot that retained volume when no longer needed, or it continues to incur storage charges. Also inspect externally supplied NAT, endpoints, buckets, and snapshots; this Terraform configuration does not own or remove them.

The template is a single-host demo and has not been applied from this workspace. Test recovery separately before considering a deployment production-ready.
