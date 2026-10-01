import hashlib
import io
import uuid

import pandas as pd

from src.adapters.messages import REQUIRED_COLUMNS, parse_transaction
from src.domain.entities import InvalidData, Transaction


def read_transactions(content: bytes, limit: int | None = None) -> list[Transaction]:
    if limit is not None and limit < 1:
        raise InvalidData("CSV row limit must be positive")
    frame = pd.read_csv(io.BytesIO(content), dtype=object, nrows=limit)
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise InvalidData("Missing CSV columns: " + ", ".join(sorted(missing)))
    if frame.empty:
        raise InvalidData("CSV contains no transactions")
    records = frame.where(pd.notna(frame), None).to_dict("records")
    digest = hashlib.sha256(content).hexdigest()
    transactions = []
    for index, record in enumerate(records):
        if record.get("transaction_id") is None:
            record["transaction_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{digest}:{index}"))
        try:
            transactions.append(parse_transaction(record))
        except InvalidData as exc:
            raise InvalidData(f"CSV row {index + 2}: {exc}") from exc
    ids = [transaction.transaction_id for transaction in transactions]
    if len(set(ids)) != len(ids):
        raise InvalidData("CSV contains duplicate transaction_id values")
    return transactions
