from collections.abc import Callable, Iterable
from dataclasses import dataclass

from src.application.ports import FraudPredictor, ResultsReader, ScorePublisher, ScoreWriter, TransactionPublisher
from src.domain.entities import FraudPolicy, ResultsSnapshot, ScoreResult, Transaction


@dataclass
class ScoreTransaction:
    predictor: FraudPredictor
    policy: FraudPolicy

    def execute(self, transaction: Transaction) -> ScoreResult:
        probability = self.predictor.predict(transaction)
        return self.policy.classify(transaction.transaction_id, probability)


@dataclass
class ProcessTransaction:
    scorer: ScoreTransaction
    publisher: ScorePublisher

    def execute(self, transaction: Transaction) -> ScoreResult:
        result = self.scorer.execute(transaction)
        self.publisher.publish(result)
        return result


@dataclass
class StoreScore:
    writer: ScoreWriter

    def execute(self, result: ScoreResult) -> None:
        self.writer.save(result)


class SubmissionFailed(RuntimeError):
    def __init__(self, sent: int):
        self.sent = sent
        super().__init__(f"Submission interrupted after {sent} acknowledged transactions")


@dataclass
class SubmitTransactions:
    publisher: TransactionPublisher

    def execute(self, transactions: Iterable[Transaction], on_progress: Callable[[int], None] | None = None) -> int:
        sent = 0
        for transaction in transactions:
            try:
                self.publisher.publish(transaction)
            except Exception as exc:
                raise SubmissionFailed(sent) from exc
            sent += 1
            if on_progress is not None:
                on_progress(sent)
        return sent


@dataclass
class GetResults:
    reader: ResultsReader

    def execute(self) -> ResultsSnapshot:
        return self.reader.read(fraud_limit=10, scores_limit=100)
