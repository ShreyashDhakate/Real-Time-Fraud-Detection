"""Real Kafka worker process-kill trials. Run as: python -m scripts.crash_trials.

Requires local Kafka/Redis and the dev dependencies. Does not substitute for
broker/host failure tests. Uses isolated topics and deletes only its own topics.
"""

import argparse
from collections import Counter
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from fraud_pipeline.config import Settings
from fraud_pipeline.data import synthetic
from fraud_pipeline.store import FeatureStore
from fraud_pipeline.streaming import consumer_config, init_topics, produce_events, restore
from tests.test_features import reference


def collect(settings, expected_count, worker, timeout=120):
    consumer = Consumer(consumer_config(settings, f"audit-{uuid.uuid4()}"))
    consumer.assign([TopicPartition(settings.topic("features"), 0, 0)])
    outputs, deadline = [], time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if worker.poll() is not None:
                raise RuntimeError(f"Recovery worker exited with {worker.returncode}; inspect trial log")
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    if len(outputs) >= expected_count:
                        return outputs
                    continue
                raise RuntimeError(message.error())
            outputs.append(json.loads(message.value()))
        raise TimeoutError(f"Expected {expected_count} outputs; received {len(outputs)}")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--output", default="reports/crash-trials.json")
    parser.add_argument("--keep-topics", action="store_true")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("trials must be positive")
    results = []
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    for trial in range(args.trials):
        settings = replace(Settings(), namespace=f"crash-{uuid.uuid4().hex[:16]}", partitions=1, partition=0)
        init_topics(settings)
        # Known finite input, retries, and active windows spanning many batches.
        events = list(synthetic(120, 4, trial, start=time.time() - 120, rate=2))
        input_events = [row for i, row in enumerate(events) for _ in range(2 if i % 9 == 0 else 1)]
        produce_events(settings, input_events)
        point = "before_commit" if trial % 2 == 0 else "after_commit"
        env = {
            **os.environ,
            "PIPELINE_NAMESPACE": settings.namespace,
            "WORKER_PARTITION": "0",
            "KAFKA_PARTITIONS": "1",
            "BATCH_SIZE": "10",
            "METRICS_PORT": "0",
            "CRASH_POINT": point,
            "CRASH_AFTER_BATCH": str(1 + trial % 5),
        }
        command = [sys.executable, "-m", "fraud_pipeline", "worker"]
        worker = None
        result = {
            "trial": trial,
            "seed": trial,
            "namespace": settings.namespace,
            "point": point,
            "input_events": len(input_events),
            "unique_events": len(events),
            "passed": False,
        }
        try:
            with output.with_name(f"crash-{trial}.log").open("w") as log:
                worker = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                code = worker.wait(timeout=120)
                if code != 137:
                    raise RuntimeError(f"Expected injected exit 137, got {code}")
                env.pop("CRASH_POINT")
                worker = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                outputs = collect(settings, len(events), worker)
                counts = Counter(row["transaction_id"] for row in outputs)
                assert counts == Counter(row.transaction_id for row in events), (
                    "Missing/duplicate committed output"
                )
                expected = reference(events)
                for actual, wanted in zip(outputs, expected, strict=True):
                    for name, value in wanted.items():
                        if value is None:
                            assert actual["features"][name] is None, name
                        else:
                            import math

                            assert math.isclose(
                                actual["features"][name], value, rel_tol=1e-5, abs_tol=1e-5
                            ), name
                restored, checkpoint = restore(settings)
                assert checkpoint["next_offset"] == len(input_events), "Checkpoint did not reach full input"
                for entity in {row.entity_id for row in events}:
                    state = restored.state[entity]
                    entity_events = [row for row in events if row.entity_id == entity]
                    assert state["history"] == [row.to_dict() for row in entity_events]
                    assert set(state["seen"]) == {row.transaction_id for row in entity_events}
                # Replay the materialization boundary twice; validates actual Redis
                # idempotency, but is not a materializer process-kill experiment.
                store = FeatureStore(settings.redis_url, settings.namespace, ttl=300)
                for row in outputs:
                    store.put(row)
                    store.put(row)
                    assert store.get(row["entity_id"], row["transaction_id"]) == row
                result.update(
                    passed=True,
                    outputs=len(outputs),
                    missing=0,
                    double_counted=0,
                    checkpoint_next_offset=checkpoint["next_offset"],
                )
        except Exception as error:
            result["error"] = repr(error)
        finally:
            if worker is not None and worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=10)
            results.append(result)
            output.write_text(
                json.dumps(
                    {
                        "scope": "worker process exits before/after Kafka commit; Redis duplicate replay",
                        "trials": results,
                    },
                    indent=2,
                )
            )
            print(json.dumps(result), flush=True)
            if not args.keep_topics and result["passed"]:
                admin = AdminClient({"bootstrap.servers": settings.brokers})
                for future in admin.delete_topics(
                    [settings.topic(x) for x in ("transactions", "features", "state", "rejected")]
                ).values():
                    future.result()
        if not result["passed"]:
            raise SystemExit("Trial failed; evidence and isolated topics retained")


if __name__ == "__main__":
    main()
