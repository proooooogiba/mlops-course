import math
from dataclasses import dataclass
from datetime import datetime


class InvalidData(ValueError):
    """Expected rejection of an incoming transaction or score."""


def validate_id(value: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 256:
        raise InvalidData("transaction_id must contain 1 to 256 characters without surrounding whitespace")


def finite_number(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidData(f"{name} must be a finite number")


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    transaction_time: datetime
    amount: float | None
    population_city: float | None
    lat: float
    lon: float
    merchant_lat: float
    merchant_lon: float
    gender: str | None
    merch: str | None
    cat_id: str | None
    one_city: str | None
    us_state: str | None
    jobs: str | None

    def __post_init__(self):
        validate_id(self.transaction_id)
        if not isinstance(self.transaction_time, datetime):
            raise InvalidData("transaction_time must be a datetime")
        for name in ("amount", "population_city"):
            value = getattr(self, name)
            if value is not None:
                finite_number(value, name)
                if value < 0:
                    raise InvalidData(f"{name} must be nonnegative")
        for name, bound in (("lat", 90), ("lon", 180), ("merchant_lat", 90), ("merchant_lon", 180)):
            value = getattr(self, name)
            finite_number(value, name)
            if not -bound <= value <= bound:
                raise InvalidData("Coordinates are outside geographic bounds")
        for name in ("gender", "merch", "cat_id", "one_city", "us_state", "jobs"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise InvalidData(f"{name} must be a string or null")


@dataclass(frozen=True)
class ScoreResult:
    transaction_id: str
    score: float
    fraud_flag: int

    def __post_init__(self):
        validate_id(self.transaction_id)
        finite_number(self.score, "score")
        if not 0 <= self.score <= 1:
            raise InvalidData("Score must be in [0, 1]")
        if type(self.fraud_flag) is not int or self.fraud_flag not in (0, 1):
            raise InvalidData("fraud_flag must be the integer 0 or 1")


@dataclass(frozen=True)
class FraudPolicy:
    threshold: float

    def __post_init__(self):
        finite_number(self.threshold, "threshold")
        if not 0 <= self.threshold <= 1:
            raise InvalidData("Threshold must be between 0 and 1")

    def classify(self, transaction_id: str, score: float) -> ScoreResult:
        finite_number(score, "score")
        return ScoreResult(transaction_id, score, int(score > self.threshold))


@dataclass(frozen=True)
class SavedScore:
    result: ScoreResult
    received_at: datetime


@dataclass(frozen=True)
class ResultsSnapshot:
    fraud_transactions: tuple[SavedScore, ...]
    recent_scores: tuple[float, ...]
    total: int
