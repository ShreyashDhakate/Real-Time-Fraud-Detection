"""Static partition ownership with Kafka-atomic offsets, outputs, and snapshots.

One process owns one explicit partition. Do not run autoscaled subscribe-based
consumers in the same group. A stable transactional ID fences prior owners.
"""

import json
import logging
import os
import signal
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .domain import FeatureEngine, Transaction

log = logging.getLogger(__name__)


def encode(value):
    return json.dumps(value, allow_nan=False, separators=(",", ":")).encode()


def init_topics(settings):
    admin = AdminClient({"bootstrap.servers": settings.brokers})
    topics = [
        NewTopic(
            settings.topic(name),
            settings.partitions,
            1,
            config={"cleanup.policy": "compact", "max.message.bytes": "10485760"}
            if name == "state"
            else {"retention.ms": "604800000"},
        )
        for name in ("transactions", "features", "state", "rejected")
    ]
    for topic, future in admin.create_topics(topics).items():
        try:
            future.result()
            log.info("created %s", topic)
        except Exception as error:
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise


def consumer_config(settings, group):
    return {
        "bootstrap.servers": settings.brokers,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
        "max.poll.interval.ms": 300000,
    }


def restore(settings):
    reader = Consumer(consumer_config(settings, f"restore-{uuid.uuid4()}"))
    reader.assign([TopicPartition(settings.topic("state"), settings.partition, 0)])
    state, metadata = {}, None
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline:
            message = reader.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    return FeatureEngine(state, (metadata or {}).get("watermark", 0)), metadata
                raise RuntimeError(message.error())
            key = message.key().decode()
            if key == "__checkpoint__":
                metadata = json.loads(message.value())
            elif message.value() is None:
                state.pop(key[2:], None)
            else:
                state[key[2:]] = json.loads(message.value())
        raise TimeoutError("State restore did not reach the committed log end within 120s")
    finally:
        reader.close()


def crash_hook(point, batch):
    if os.getenv("CRASH_POINT") == point and batch == int(os.getenv("CRASH_AFTER_BATCH", "1")):
        os._exit(137)  # Intentionally bypass cleanup for the integration fault harness.


def run_worker(settings):
    from prometheus_client import Counter, Gauge, start_http_server

    start_http_server(settings.metrics_port)
    processed = Counter("fraud_events", "Committed input events", ["status"])
    state_size = Gauge("fraud_state_entities", "Entities in this worker's state")
    producer = Producer(
        {
            "bootstrap.servers": settings.brokers,
            "transactional.id": f"{settings.group}.partition-{settings.partition}",
            "transaction.timeout.ms": 60000,
            "message.max.bytes": 10485760,
            "linger.ms": 5,
            "compression.type": "lz4",
        }
    )
    producer.init_transactions(120)  # Fence old owner before reading committed state.
    engine, checkpoint = restore(settings)
    consumer = Consumer(consumer_config(settings, settings.group))
    tp = TopicPartition(settings.topic("transactions"), settings.partition)
    committed = consumer.committed([tp], timeout=30)[0].offset
    expected = (checkpoint or {}).get("next_offset", 0)
    if (committed >= 0 and committed != expected) or (committed < 0 and checkpoint is not None):
        raise RuntimeError("Checkpoint/group offsets differ; refusing unsafe recovery")
    low, _ = consumer.get_watermark_offsets(tp, timeout=30)
    if expected < low:
        raise RuntimeError("Required source offsets expired; restore from an archive into a new namespace")
    consumer.assign([TopicPartition(tp.topic, tp.partition, expected)])
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    batch_number = 0
    try:
        while running:
            messages = consumer.consume(settings.batch_size, timeout=1)
            messages = [
                m for m in messages if not (m.error() and m.error().code() == KafkaError._PARTITION_EOF)
            ]
            if not messages:
                continue
            if any(m.error() for m in messages):
                raise RuntimeError(next(m.error() for m in messages if m.error()))
            batch_number += 1
            producer.begin_transaction()
            touched, statuses = set(), []
            try:
                for message in messages:
                    try:
                        event = Transaction(**json.loads(message.value()))
                        if message.key() != event.entity_id.encode():
                            raise ValueError("Kafka key must equal entity_id")
                        status, snapshot = engine.process(event)
                    except (ValueError, TypeError, KeyError, UnicodeDecodeError) as error:
                        status, snapshot = "invalid", None
                        event = None
                        reason = str(error)
                    statuses.append(status)
                    if snapshot:
                        touched.add(event.entity_id)
                        snapshot["version"] = f"{message.partition()}:{message.offset()}"
                        producer.produce(
                            settings.topic("features"),
                            key=event.entity_id,
                            value=encode(snapshot),
                            partition=settings.partition,
                        )
                    elif status in ("late", "invalid"):
                        producer.produce(
                            settings.topic("rejected"),
                            key=message.key(),
                            value=encode(
                                {
                                    "offset": message.offset(),
                                    "status": status,
                                    "reason": reason if status == "invalid" else "event-time policy",
                                    "raw": message.value().decode(errors="replace"),
                                }
                            ),
                            partition=settings.partition,
                        )
                expired = engine.expire()
                for entity in touched - set(expired):
                    producer.produce(
                        settings.topic("state"),
                        key=f"e:{entity}",
                        value=encode(engine.state[entity]),
                        partition=settings.partition,
                    )
                for entity in expired:
                    producer.produce(
                        settings.topic("state"), key=f"e:{entity}", value=None, partition=settings.partition
                    )
                next_offset = messages[-1].offset() + 1
                producer.produce(
                    settings.topic("state"),
                    key="__checkpoint__",
                    value=encode({"next_offset": next_offset, "watermark": engine.watermark}),
                    partition=settings.partition,
                )
                crash_hook("before_commit", batch_number)
                producer.send_offsets_to_transaction(
                    [TopicPartition(tp.topic, tp.partition, next_offset)],
                    consumer.consumer_group_metadata(),
                    30,
                )
                producer.commit_transaction(60)
                crash_hook("after_commit", batch_number)
                for status in statuses:
                    processed.labels(status).inc()
                state_size.set(len(engine.state))
                log.info(
                    "committed partition=%s next_offset=%s events=%s",
                    tp.partition,
                    next_offset,
                    len(messages),
                )
            except BaseException:
                # Exit instead of reusing mutated local state. On restart, restore
                # from committed Kafka state even if commit outcome was ambiguous.
                log.exception("transaction failed; worker must restore before retry")
                raise
    finally:
        consumer.close()


def produce_events(settings, events, rate=0):
    producer = Producer(
        {
            "bootstrap.servers": settings.brokers,
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
        }
    )
    failures = []

    def delivered(error, _):
        if error:
            failures.append(str(error))

    start, count = time.perf_counter(), 0
    for count, event in enumerate(events, 1):
        while True:
            try:
                producer.produce(
                    settings.topic("transactions"),
                    key=event.entity_id,
                    value=encode(event.to_dict()),
                    on_delivery=delivered,
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if rate:
            time.sleep(max(0, start + count / rate - time.perf_counter()))
    remaining = producer.flush(120)
    if failures or remaining:
        raise RuntimeError(f"Delivery failures: {failures[:3]}, undelivered={remaining}")
    return {"delivered": count, "seconds": time.perf_counter() - start}
