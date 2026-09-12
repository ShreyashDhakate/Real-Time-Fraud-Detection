from dataclasses import dataclass
import os


@dataclass
class Settings:
    brokers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    namespace: str = os.getenv("PIPELINE_NAMESPACE", "fraud")
    partitions: int = int(os.getenv("KAFKA_PARTITIONS", "4"))
    partition: int = int(os.getenv("WORKER_PARTITION", "0"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    feature_ttl: int = int(os.getenv("FEATURE_TTL_SECONDS", "86400"))
    model_dir: str = os.getenv("MODEL_DIR", "artifacts/model")
    grpc_port: int = int(os.getenv("GRPC_PORT", "50051"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "8000"))

    def topic(self, suffix):
        return f"{self.namespace}.{suffix}"

    @property
    def group(self):
        return f"{self.namespace}.features.v1"
