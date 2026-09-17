"""Execute the Redis CLI with an injected GET; these are not latency measurements."""

import json
from pathlib import Path
import runpy
import sys
from threading import Lock

import pytest
import redis

from fraud_pipeline.data import synthetic, write_jsonl


SCRIPT = Path(__file__).resolve().parents[1] / "benchmarks/redis_reads.py"


def run_benchmark(monkeypatch, tmp_path, reads=100, concurrency=1, misses=0, unavailable=False):
    source = tmp_path / "transactions.jsonl"
    output = tmp_path / "result.json"
    write_jsonl(source, [event.to_dict() for event in synthetic(10)])
    calls = 0
    lock = Lock()

    def get(_client, _key):
        nonlocal calls
        with lock:
            calls += 1
            if unavailable:
                raise redis.exceptions.ConnectionError("injected unavailable store")
            return None if calls <= misses else '{"fixture":true}'

    monkeypatch.setattr(redis.Redis, "get", get)
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), "--input", str(source), "--output", str(output),
        "--reads", str(reads), "--concurrency", str(concurrency),
    ])
    failure = None
    try:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    except (SystemExit, redis.exceptions.ConnectionError) as exc:
        failure = exc
    return json.loads(output.read_text()) if output.exists() else None, failure, calls


@pytest.mark.parametrize("misses,valid", [(0, True), (1, True), (2, False)])
def test_hit_rate_gate_preserves_report(monkeypatch, tmp_path, misses, valid):
    report, failure, calls = run_benchmark(monkeypatch, tmp_path, misses=misses)
    assert calls == report["reads"] == 100
    assert report["hit_rate"] == (100 - misses) / 100
    assert set(report["latency_ms"]) == {"p50", "p95", "p99"}
    if valid:
        assert failure is None
    else:
        assert isinstance(failure, SystemExit)
        assert "fewer than 99%" in str(failure)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="CLI truncates reads to a multiple of concurrency")
def test_nondivisible_read_budget_is_fully_executed(monkeypatch, tmp_path):
    report, failure, calls = run_benchmark(monkeypatch, tmp_path, reads=17, concurrency=4)
    assert failure is None
    assert calls == report["reads"] == 17


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="Redis errors abort without a partial report")
def test_unavailable_store_retains_failure_evidence(monkeypatch, tmp_path):
    report, failure, calls = run_benchmark(monkeypatch, tmp_path, unavailable=True)
    assert calls > 0
    assert failure is not None
    assert report is not None, "A failed run must retain machine-readable partial accounting"
