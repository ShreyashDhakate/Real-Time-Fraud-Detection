import pytest

from fraud_pipeline.data import batch_rows
from fraud_pipeline.domain import Transaction


def test_batch_rejects_global_disorder_across_entities():
    events = [Transaction("1", "a", 20, 1), Transaction("2", "b", 10, 1)]
    with pytest.raises(ValueError, match="globally sorted"):
        list(batch_rows(events))
