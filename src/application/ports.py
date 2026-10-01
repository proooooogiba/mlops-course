from typing import Protocol

from src.domain.entities import ResultsSnapshot, ScoreResult, Transaction


class FraudPredictor(Protocol):
    def predict(self, transaction: Transaction) -> float: ...


class ScorePublisher(Protocol):
    def publish(self, result: ScoreResult) -> None: ...


class TransactionPublisher(Protocol):
    def publish(self, transaction: Transaction) -> None: ...


class ScoreWriter(Protocol):
    def save(self, result: ScoreResult) -> None: ...


class ResultsReader(Protocol):
    def read(self, fraud_limit: int, scores_limit: int) -> ResultsSnapshot: ...
