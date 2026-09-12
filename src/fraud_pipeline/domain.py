"""Dependency-free event contract and deterministic feature definitions."""

from dataclasses import asdict, dataclass
import heapq
import math

FEATURE_NAMES = [
    "count_1m",
    "count_5m",
    "count_1h",
    "spend_1m",
    "spend_5m",
    "spend_1h",
    "mean_5m",
    "max_1h",
    "merchants_1h",
    "seconds_since_previous",
    "distance_km",
    "speed_kmh",
]
MODEL_COLUMNS = ["amount", *FEATURE_NAMES]
SCHEMA_VERSION = 1
HISTORY_SECONDS = 3600
DEDUP_SECONDS = 86400


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    entity_id: str
    timestamp: float
    amount: float
    merchant_id: str = ""
    currency: str = "USD"
    latitude: float | None = None
    longitude: float | None = None

    def __post_init__(self):
        for field in ("transaction_id", "entity_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value or len(value) > 200:
                raise ValueError(f"{field} must be a nonempty string of at most 200 characters")
        for field in ("timestamp", "amount"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{field} must be finite")
            if value < 0:
                raise ValueError(f"{field} must be nonnegative")
        if self.currency != "USD":
            raise ValueError("amount must already be normalized to USD")
        if not isinstance(self.merchant_id, str):
            raise ValueError("merchant_id must be a string")
        if (self.latitude is None) != (self.longitude is None):
            raise ValueError("latitude and longitude must be supplied together")
        if self.latitude is not None:
            if not all(
                isinstance(v, (float, int)) and math.isfinite(v) for v in (self.latitude, self.longitude)
            ):
                raise ValueError("coordinates must be finite numbers")
            if not -90 <= self.latitude <= 90 or not -180 <= self.longitude <= 180:
                raise ValueError("coordinates out of range")

    def to_dict(self):
        return asdict(self)


def distance_km(a: Transaction, b: Transaction) -> float | None:
    if a.latitude is None or b.latitude is None:
        return None
    lat1, lat2 = math.radians(a.latitude), math.radians(b.latitude)
    dlat = lat2 - lat1
    dlon = math.radians(b.longitude - a.longitude)
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(min(1.0, max(0.0, h))))


def features(history: list[Transaction], current: Transaction) -> dict:
    """Windows are [t-window, t); equal-timestamp peers are excluded."""
    windows = [
        [x for x in history if current.timestamp - size <= x.timestamp < current.timestamp]
        for size in (60, 300, 3600)
    ]
    counts = [len(w) for w in windows]
    sums = [sum(x.amount for x in w) for w in windows]
    prior = [x for x in history if x.timestamp < current.timestamp]
    previous = prior[-1] if prior else None
    elapsed = current.timestamp - previous.timestamp if previous else None
    distance = distance_km(previous, current) if previous else None
    values = [
        *counts,
        *sums,
        sums[1] / counts[1] if counts[1] else None,
        max((x.amount for x in windows[2]), default=None),
        len({x.merchant_id for x in windows[2] if x.merchant_id}),
        elapsed,
        distance,
        distance / elapsed * 3600 if distance is not None and elapsed else None,
    ]
    return dict(zip(FEATURE_NAMES, values, strict=True))


class FeatureEngine:
    """Partition-local state; zero allowed lateness per entity, 1h across entities.

    Full touched-entity snapshots are checkpointed. This prioritizes inspectable
    correctness; large/hot entity histories require profiling and optimization.
    """

    def __init__(self, state=None, watermark=0.0):
        self.state = state or {}
        self.watermark = watermark
        self.expiry = [(value["last_ts"], key) for key, value in self.state.items()]
        heapq.heapify(self.expiry)

    def process(self, event: Transaction):
        if event.timestamp < self.watermark - HISTORY_SECONDS:
            return "late", None
        state = self.state.get(event.entity_id, {"history": [], "seen": {}, "last_ts": -1})
        if state["seen"].get(event.transaction_id, -DEDUP_SECONDS - 1) >= event.timestamp - DEDUP_SECONDS:
            return "duplicate", None
        if event.timestamp < state["last_ts"]:
            return "late", None
        history = [
            Transaction(**x) for x in state["history"] if x["timestamp"] >= event.timestamp - HISTORY_SECONDS
        ]
        result = {
            "schema_version": SCHEMA_VERSION,
            "transaction_id": event.transaction_id,
            "entity_id": event.entity_id,
            "timestamp": event.timestamp,
            "amount": event.amount,
            "features": features(history, event),
        }
        state["history"] = [x.to_dict() for x in history] + [event.to_dict()]
        state["seen"] = {k: v for k, v in state["seen"].items() if v >= event.timestamp - DEDUP_SECONDS}
        state["seen"][event.transaction_id] = event.timestamp
        state["last_ts"] = event.timestamp
        self.state[event.entity_id] = state
        heapq.heappush(self.expiry, (event.timestamp, event.entity_id))
        self.watermark = max(self.watermark, event.timestamp)
        return "accepted", result

    def expire(self):
        expired = []
        while self.expiry and self.expiry[0][0] < self.watermark - DEDUP_SECONDS:
            timestamp, key = heapq.heappop(self.expiry)
            if key in self.state and self.state[key]["last_ts"] == timestamp:
                del self.state[key]
                expired.append(key)
        return expired
