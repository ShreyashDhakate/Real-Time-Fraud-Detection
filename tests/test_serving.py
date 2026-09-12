from concurrent.futures import ThreadPoolExecutor
import time

import grpc
import pytest
import redis

from fraud_pipeline.domain import FeatureEngine, Transaction
from fraud_pipeline.generated import fraud_pb2, fraud_pb2_grpc
from fraud_pipeline.model import RiskModel
from fraud_pipeline.serving import Scorer


class MemoryStore:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def get(self, entity, transaction):
        if entity == "unavailable":
            raise redis.exceptions.ConnectionError()
        return self.snapshot if transaction == "known" else None


@pytest.fixture
def stub(tmp_path):
    snapshot = FeatureEngine().process(Transaction("known", "a", time.time(), 20))[1]
    snapshot["version"] = "0:1"
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    scorer = Scorer(MemoryStore(snapshot), RiskModel(tmp_path, allow_demo=True))
    fraud_pb2_grpc.add_FraudScorerServicer_to_server(scorer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        yield fraud_pb2_grpc.FraudScorerStub(channel), snapshot
    server.stop(0).wait()


def test_real_grpc_roundtrip(stub):
    client, _ = stub
    response = client.Score(fraud_pb2.ScoreRequest(entity_id="a", transaction_id="known"), timeout=2)
    assert 0 <= response.risk_score <= 1
    assert response.demo_model
    assert response.feature_version == "0:1"


@pytest.mark.parametrize(
    "entity,transaction,expected",
    [
        ("", "known", grpc.StatusCode.INVALID_ARGUMENT),
        ("a", "missing", grpc.StatusCode.NOT_FOUND),
        ("unavailable", "known", grpc.StatusCode.UNAVAILABLE),
    ],
)
def test_rpc_failures(stub, entity, transaction, expected):
    client, _ = stub
    with pytest.raises(grpc.RpcError) as error:
        client.Score(fraud_pb2.ScoreRequest(entity_id=entity, transaction_id=transaction), timeout=2)
    assert error.value.code() == expected


def test_stale_snapshot(stub):
    client, snapshot = stub
    snapshot["timestamp"] -= 90000
    with pytest.raises(grpc.RpcError) as error:
        client.Score(fraud_pb2.ScoreRequest(entity_id="a", transaction_id="known"), timeout=2)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_demo_requires_explicit_opt_in(tmp_path):
    with pytest.raises(FileNotFoundError):
        RiskModel(tmp_path)
