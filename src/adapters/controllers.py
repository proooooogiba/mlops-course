from dataclasses import dataclass

from src.adapters.messages import parse_score, parse_transaction
from src.application.use_cases import ProcessTransaction, StoreScore


@dataclass(frozen=True)
class MessageSource:
    topic: str
    partition: int
    offset: int

    @property
    def identity(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass
class TransactionController:
    use_case: ProcessTransaction

    def handle(self, record, source: MessageSource):
        return self.use_case.execute(parse_transaction(record, fallback_id=source.identity))


@dataclass
class ScoreController:
    use_case: StoreScore

    def handle(self, record, _source: MessageSource):
        self.use_case.execute(parse_score(record))
