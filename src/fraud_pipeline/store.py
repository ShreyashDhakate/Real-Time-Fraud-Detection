import hashlib
import json
import signal
import time

import redis

from .domain import SCHEMA_VERSION

# Each key is an immutable transaction snapshot. A retry can refresh TTL but a
# conflicting source offset/payload cannot silently replace its feature history.
APPLY = """
local old = redis.call('GET', KEYS[1])
if old and old ~= ARGV[1] then return -1 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return old and 0 or 1
"""


class FeatureStore:
    def __init__(self, url, namespace="fraud", ttl=86400):
        self.client = redis.Redis.from_url(
            url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2
        )
        self.namespace, self.ttl = namespace, ttl
        self.apply_script = self.client.register_script(APPLY)

    def key(self, entity, transaction):
        digest = hashlib.sha256(json.dumps([entity, transaction]).encode()).hexdigest()
        return f"{self.namespace}:v{SCHEMA_VERSION}:transaction:{digest}"

    def put(self, snapshot):
        if snapshot["schema_version"] != SCHEMA_VERSION:
            raise ValueError("Incompatible feature schema")
        payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False)
        result = self.apply_script(
            keys=[self.key(snapshot["entity_id"], snapshot["transaction_id"])], args=[payload, self.ttl]
        )
        if result == -1:
            raise ValueError(
                "Conflicting immutable feature snapshot; inspect duplicate IDs or namespace reuse"
            )
        return result

    def get(self, entity, transaction):
        raw = self.client.get(self.key(entity, transaction))
        return json.loads(raw) if raw else None


def run_materializer(settings):
    from confluent_kafka import Consumer, KafkaError
    from .streaming import consumer_config

    store = FeatureStore(settings.redis_url, settings.namespace, settings.feature_ttl)
    consumer = Consumer(consumer_config(settings, f"{settings.namespace}.redis.v1"))
    consumer.subscribe([settings.topic("features")])
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while running:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            snapshot = json.loads(message.value())
            while running:
                try:
                    store.put(snapshot)
                    # If killed here, reapplying the exact value is harmless.
                    consumer.commit(message=message, asynchronous=False)
                    break
                except redis.exceptions.ConnectionError:
                    time.sleep(1)
    finally:
        consumer.close()
