from dataclasses import replace
from datetime import datetime

import pytest

from src.adapters.controllers import MessageSource, ScoreController, TransactionController
from src.adapters.messages import transaction_message
from src.application.use_cases import GetResults, ProcessTransaction, ScoreTransaction, StoreScore, SubmissionFailed, SubmitTransactions
from src.domain.entities import FraudPolicy, InvalidData, ResultsSnapshot, ScoreResult, Transaction


@pytest.fixture
def transaction():
    return Transaction("tx-1", datetime(2023, 1, 1, 12), 12.0, 50000.0,
                       40.71, -74.0, 40.72, -74.01, "M", "shop", "shopping_net", "New York", "NY", "Engineer")


class FixedPredictor:
    def __init__(self, score):
        self.score = score
        self.calls = []

    def predict(self, transaction):
        self.calls.append(transaction)
        return self.score


class RecordingPublisher:
    def __init__(self):
        self.items = []

    def publish(self, item):
        self.items.append(item)


@pytest.mark.parametrize("probability,expected", [(0.979, 0), (0.98, 0), (0.981, 1)])
def test_threshold_is_domain_policy(transaction, probability, expected):
    predictor = FixedPredictor(probability)
    result = ScoreTransaction(predictor, FraudPolicy(0.98)).execute(transaction)
    assert result == ScoreResult("tx-1", probability, expected)
    assert predictor.calls == [transaction]


def test_process_publishes_model_result(transaction):
    publisher = RecordingPublisher()
    scenario = ProcessTransaction(ScoreTransaction(FixedPredictor(0.99), FraudPolicy(0.98)), publisher)
    result = scenario.execute(transaction)
    assert publisher.items == [result]
    assert result == ScoreResult("tx-1", 0.99, 1)


def test_prediction_failure_does_not_publish(transaction):
    class BrokenPredictor:
        def predict(self, transaction):
            raise RuntimeError("model unavailable")
    publisher = RecordingPublisher()
    scenario = ProcessTransaction(ScoreTransaction(BrokenPredictor(), FraudPolicy(0.98)), publisher)
    with pytest.raises(RuntimeError, match="model unavailable"):
        scenario.execute(transaction)
    assert publisher.items == []


def test_publication_failure_propagates_to_consumer(transaction):
    class UnavailablePublisher:
        def publish(self, result):
            raise ConnectionError("broker unavailable")
    scenario = ProcessTransaction(ScoreTransaction(FixedPredictor(0.99), FraudPolicy(0.98)), UnavailablePublisher())
    with pytest.raises(ConnectionError, match="broker unavailable"):
        scenario.execute(transaction)


def test_store_delegates_result_to_storage_port():
    class Writer:
        def __init__(self):
            self.saved = []
        def save(self, result):
            self.saved.append(result)
    writer = Writer()
    result = ScoreResult("tx-1", 0.99, 1)
    StoreScore(writer).execute(result)
    assert writer.saved == [result]


def test_storage_failure_propagates_to_consumer():
    class UnavailableWriter:
        def save(self, result):
            raise ConnectionError("database unavailable")
    with pytest.raises(ConnectionError, match="database unavailable"):
        StoreScore(UnavailableWriter()).execute(ScoreResult("tx-1", 0.99, 1))


def test_submit_reports_only_acknowledged_transactions(transaction):
    class InterruptedPublisher:
        def __init__(self):
            self.items = []
        def publish(self, item):
            if self.items:
                raise ConnectionError("broker unavailable")
            self.items.append(item)
    publisher = InterruptedPublisher()
    progress = []
    with pytest.raises(SubmissionFailed) as caught:
        SubmitTransactions(publisher).execute([transaction, replace(transaction, transaction_id="tx-2")], progress.append)
    assert caught.value.sent == 1
    assert progress == [1]
    assert publisher.items == [transaction]
    assert isinstance(caught.value.__cause__, ConnectionError)


def test_dashboard_requests_business_limits():
    class Reader:
        def read(self, fraud_limit, scores_limit):
            assert fraud_limit == 10
            assert scores_limit == 100
            return ResultsSnapshot((), (), 0)
    assert GetResults(Reader()).execute() == ResultsSnapshot((), (), 0)


def test_transaction_controller_assigns_stable_source_id(transaction):
    publisher = RecordingPublisher()
    use_case = ProcessTransaction(ScoreTransaction(FixedPredictor(0.5), FraudPolicy(0.98)), publisher)
    payload = transaction_message(transaction)
    del payload["transaction_id"]
    result = TransactionController(use_case).handle(payload, MessageSource("transactions", 2, 41))
    assert result.transaction_id == "transactions:2:41"
    assert publisher.items == [result]


def test_score_controller_rejects_invalid_payload_before_storage():
    class Writer:
        def save(self, result):
            pytest.fail("invalid score must not reach storage")
    with pytest.raises(InvalidData):
        ScoreController(StoreScore(Writer())).handle({"transaction_id": "x", "score": 2, "fraud_flag": 1}, MessageSource("scores", 0, 0))
