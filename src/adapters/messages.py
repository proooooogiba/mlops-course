import math
from dataclasses import asdict

import pandas as pd

from src.domain.entities import InvalidData, ScoreResult, Transaction

CATEGORIES = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
REQUIRED_COLUMNS = {"transaction_time", "amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon", *CATEGORIES}


def parse_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise InvalidData("transaction_id must be a string or integer")
    return str(value).strip()


def parse_number(value, name, optional=False):
    if value is None or value == "" or (isinstance(value, float) and math.isnan(value)):
        if optional:
            return None
        raise InvalidData(f"Missing numeric field: {name}")
    if isinstance(value, bool):
        raise InvalidData(f"Invalid numeric field: {name}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidData(f"Invalid numeric field: {name}") from exc


def parse_category(value, name):
    if value is None or value == "" or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (str, int, float)):
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and float(value).is_integer():
            return str(int(value))
        return str(value)
    raise InvalidData(f"Invalid categorical field: {name}")


def parse_transaction(record, fallback_id=None) -> Transaction:
    if not isinstance(record, dict):
        raise InvalidData("Transaction must be a JSON object")
    missing = REQUIRED_COLUMNS - record.keys()
    if missing:
        raise InvalidData("Missing fields: " + ", ".join(sorted(missing)))
    try:
        timestamp = pd.Timestamp(record["transaction_time"])
        if pd.isna(timestamp):
            raise InvalidData("Missing transaction_time")
        timestamp = timestamp.to_pydatetime()
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidData("Invalid transaction_time") from exc
    return Transaction(
        transaction_id=parse_id(record.get("transaction_id", fallback_id)),
        transaction_time=timestamp,
        **{name: parse_number(record[name], name, optional=name in ("amount", "population_city"))
           for name in ("amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon")},
        **{name: parse_category(record[name], name) for name in CATEGORIES},
    )


def parse_score(record) -> ScoreResult:
    if not isinstance(record, dict) or set(record) != {"transaction_id", "score", "fraud_flag"}:
        raise InvalidData("Score must have exactly transaction_id, score, fraud_flag")
    return ScoreResult(parse_id(record["transaction_id"]), record["score"], record["fraud_flag"])


def transaction_message(transaction: Transaction) -> dict:
    record = asdict(transaction)
    record["transaction_time"] = transaction.transaction_time.isoformat()
    return record


def score_message(result: ScoreResult) -> dict:
    return asdict(result)
