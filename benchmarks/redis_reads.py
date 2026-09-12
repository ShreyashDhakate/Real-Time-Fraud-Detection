"""Client-observed Redis GET latency with concurrent readers; requires populated data."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import platform
import random
import time

import numpy as np

from fraud_pipeline.config import Settings
from fraud_pipeline.data import read_events
from fraud_pipeline.store import FeatureStore

parser = argparse.ArgumentParser()
parser.add_argument("--input", default="data/transactions.jsonl")
parser.add_argument("--reads", type=int, default=100000)
parser.add_argument("--concurrency", type=int, default=16)
parser.add_argument("--output", default="reports/redis.json")
args = parser.parse_args()
if args.concurrency < 1 or args.reads < args.concurrency:
    parser.error("reads must be at least concurrency, and concurrency must be positive")
settings = Settings()
events = list(read_events(args.input))
if not events:
    parser.error("input must contain at least one transaction")
store = FeatureStore(settings.redis_url, settings.namespace)


def read(seed):
    rng = random.Random(seed)
    samples, hits, payload_bytes = [], 0, 0
    for _ in range(args.reads // args.concurrency):
        event = rng.choice(events)
        key = store.key(event.entity_id, event.transaction_id)
        start = time.perf_counter_ns()
        value = store.client.get(key)
        samples.append((time.perf_counter_ns() - start) / 1e6)
        hits += value is not None
        payload_bytes += len(value.encode()) if value else 0
    return samples, hits, payload_bytes


start = time.perf_counter()
with ThreadPoolExecutor(args.concurrency) as pool:
    results = list(pool.map(read, range(args.concurrency)))
samples = [sample for result in results for sample in result[0]]
hits = sum(result[1] for result in results)
report = {
    "reads": len(samples),
    "hit_rate": hits / len(samples),
    "concurrency": args.concurrency,
    "latency_ms": dict(zip(["p50", "p95", "p99"], np.percentile(samples, [50, 95, 99]).tolist())),
    "seconds": time.perf_counter() - start,
    "platform": platform.platform(),
    "mean_hit_payload_bytes": sum(result[2] for result in results) / hits if hits else 0,
}
Path(args.output).parent.mkdir(parents=True, exist_ok=True)
Path(args.output).write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
if report["hit_rate"] < 0.99:
    raise SystemExit("Invalid benchmark: fewer than 99% of requested features were present")
