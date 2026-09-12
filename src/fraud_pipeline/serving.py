from concurrent import futures
import os
import time

import grpc
import redis
from prometheus_client import Counter, Histogram, start_http_server

from .generated import fraud_pb2, fraud_pb2_grpc
from .model import RiskModel
from .store import FeatureStore


class Scorer(fraud_pb2_grpc.FraudScorerServicer):
    def __init__(self, store, model, max_age=86400):
        self.store, self.model, self.max_age = store, model, max_age

    def Score(self, request, context):
        if (
            not request.entity_id
            or not request.transaction_id
            or max(len(request.entity_id), len(request.transaction_id)) > 200
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "entity_id and transaction_id are required, max 200 chars"
            )
        try:
            snapshot = self.store.get(request.entity_id, request.transaction_id)
        except redis.exceptions.RedisError:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Feature store unavailable")
        if snapshot is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Transaction snapshot not materialized or expired")
        age = time.time() - snapshot["timestamp"]
        if age < -60:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Transaction timestamp is in the future")
        if age > self.max_age:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Transaction snapshot exceeds maximum age")
        if not context.is_active():
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Request deadline elapsed")
        return fraud_pb2.ScoreResponse(
            risk_score=self.model.predict(snapshot),
            model_version=self.model.version,
            feature_version=snapshot["version"],
            feature_age_seconds=max(0, age),
            demo_model=self.model.demo,
        )


class MetricsInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        self.requests = Counter("fraud_grpc_requests", "Scoring RPC outcomes", ["code"])
        self.latency = Histogram(
            "fraud_grpc_seconds",
            "Server-side RPC duration",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 5),
        )

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        def invoke(request, context):
            with self.latency.time():
                try:
                    return handler.unary_unary(request, context)
                finally:
                    self.requests.labels(str(context.code() or grpc.StatusCode.OK)).inc()

        return grpc.unary_unary_rpc_method_handler(
            invoke,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def run_server(settings):
    model = RiskModel(settings.model_dir, allow_demo=os.getenv("ALLOW_DEMO_MODEL", "0") == "1")
    store = FeatureStore(settings.redis_url, settings.namespace, settings.feature_ttl)
    store.client.ping()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        maximum_concurrent_rpcs=256,
        interceptors=[MetricsInterceptor()],
    )
    fraud_pb2_grpc.add_FraudScorerServicer_to_server(
        Scorer(store, model, float(os.getenv("MAX_FEATURE_AGE_SECONDS", "86400"))), server
    )
    address = f"[::]:{settings.grpc_port}"
    cert, key = os.getenv("GRPC_TLS_CERT"), os.getenv("GRPC_TLS_KEY")
    if bool(cert) != bool(key):
        raise ValueError("Supply both GRPC_TLS_CERT and GRPC_TLS_KEY")
    if cert:
        from pathlib import Path

        server.add_secure_port(
            address, grpc.ssl_server_credentials([(Path(key).read_bytes(), Path(cert).read_bytes())])
        )
    else:
        server.add_insecure_port(address)
    start_http_server(settings.metrics_port)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(5).wait()


def score(target, entity, transaction):
    with grpc.insecure_channel(target) as channel:
        stub = fraud_pb2_grpc.FraudScorerStub(channel)
        response = stub.Score(fraud_pb2.ScoreRequest(entity_id=entity, transaction_id=transaction), timeout=5)
    from google.protobuf.json_format import MessageToDict

    return MessageToDict(
        response, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
    )
