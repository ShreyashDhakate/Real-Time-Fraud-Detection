"""Local transport probes; injected stores do not establish Redis integration."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace

import grpc
import pytest
import redis

from fraud_pipeline import serving
from fraud_pipeline.domain import FeatureEngine, SCHEMA_VERSION, Transaction
from fraud_pipeline.generated import fraud_pb2, fraud_pb2_grpc
from fraud_pipeline.model import RiskModel


NOW = 200_000.0
REQUEST = fraud_pb2.ScoreRequest(entity_id="a", transaction_id="known")


class RecordingModel:
    def __init__(self, directory):
        self.delegate = RiskModel(directory, allow_demo=True)
        self.version, self.demo = self.delegate.version, self.delegate.demo
        self.calls = 0

    def predict(self, snapshot):
        self.calls += 1
        return self.delegate.predict(snapshot)


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    # Replace only serving's clock reference, leaving gRPC's real clock intact.
    monkeypatch.setattr(serving, "time", SimpleNamespace(time=lambda: NOW))
    snapshot = FeatureEngine().process(Transaction("known", "a", NOW, 20))[1]
    snapshot["version"] = "0:1"
    return snapshot, RecordingModel(tmp_path)


@contextmanager
def local_client(store, model):
    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        fraud_pb2_grpc.add_FraudScorerServicer_to_server(serving.Scorer(store, model), server)
        port = server.add_insecure_port("127.0.0.1:0")
        if port == 0:
            raise RuntimeError("Could not bind local gRPC test server")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                yield fraud_pb2_grpc.FraudScorerStub(channel)
        finally:
            server.stop(0).wait()


@pytest.mark.parametrize(
    "age,expected_age",
    [(86400.0, 86400.0), (86400.001, None), (-60.0, 0.0), (-60.001, None)],
)
def test_freshness_boundaries(inputs, age, expected_age):
    snapshot, model = inputs
    snapshot["timestamp"] = NOW - age
    with local_client(SimpleNamespace(get=lambda *_: snapshot), model) as client:
        if expected_age is None:
            with pytest.raises(grpc.RpcError) as error:
                client.Score(REQUEST, timeout=2)
            assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert model.calls == 0
        else:
            response = client.Score(REQUEST, timeout=2)
            assert response.feature_age_seconds == expected_age
            assert response.feature_version == "0:1"
            assert response.model_version == "demo-heuristic-v1"
            assert response.demo_model
            assert model.calls == 1


@pytest.mark.parametrize("exception", [redis.exceptions.ConnectionError, redis.exceptions.TimeoutError])
def test_store_failure_skips_inference(inputs, exception):
    _, model = inputs

    def get(*_):
        raise exception("injected store failure")

    with local_client(SimpleNamespace(get=get), model) as client:
        with pytest.raises(grpc.RpcError) as error:
            client.Score(REQUEST, timeout=2)
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        assert model.calls == 0


def test_deadline_during_store_read_skips_inference_after_read_returns(inputs):
    snapshot, model = inputs
    entered, release, finished = Event(), Event(), Event()

    def get(*_):
        entered.set()
        if not release.wait(timeout=5):
            raise RuntimeError("test did not release store read")
        return snapshot

    class ObservedScorer(serving.Scorer):
        def Score(self, request, context):
            try:
                return super().Score(request, context)
            finally:
                finished.set()

    # Observe handler completion independently of the client's deadline result.
    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        fraud_pb2_grpc.add_FraudScorerServicer_to_server(
            ObservedScorer(SimpleNamespace(get=get), model), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        assert port != 0
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                pending = fraud_pb2_grpc.FraudScorerStub(channel).Score.future(REQUEST, timeout=0.5)
                assert entered.wait(timeout=2)
                with pytest.raises(grpc.RpcError) as error:
                    pending.result(timeout=2)
                assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                assert not finished.is_set()  # Client expiry does not interrupt a blocking GET.
                release.set()
                assert finished.wait(timeout=2)
                assert model.calls == 0
        finally:
            release.set()
            server.stop(0).wait()


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Scorer currently ignores snapshot schema_version; see docs/SCORING_FAILURES.md",
)
def test_incompatible_snapshot_schema_rejected_before_inference(inputs):
    snapshot, model = inputs
    snapshot["schema_version"] = SCHEMA_VERSION + 1
    with local_client(SimpleNamespace(get=lambda *_: snapshot), model) as client:
        # Keep unexpected transport errors as real failures, not expected failures.
        try:
            response = client.Score(REQUEST, timeout=2)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
                raise
        else:
            assert False, f"Incompatible schema was scored: {response}"
        assert model.calls == 0
