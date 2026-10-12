"""Shared CSV contract and small ClickHouse HTTP client (standard library)."""

import base64
import csv
import hashlib
import json
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

CSV_FIELDS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def normalize(row: dict, source_file: str, source_row: int) -> dict:
    """Preserve all input columns, use exact cents and stable source coordinates."""
    missing = set(CSV_FIELDS) - row.keys()
    if missing or any(row.get(k) is None for k in CSV_FIELDS) or None in row:
        raise ValueError(f"missing columns or malformed CSV row: {sorted(missing)}")
    result = {k: row[k] for k in CSV_FIELDS}
    if not re.fullmatch(r"[A-Z]{2}", row["us_state"]):
        raise ValueError("us_state must be a two-letter US state code")
    if not row["cat_id"]:
        raise ValueError("cat_id must not be empty")
    try:
        amount = Decimal(row["amount"])
        if not amount.is_finite() or not 0 <= amount < Decimal("10000000000"):
            raise ValueError("amount must fit nonnegative Decimal(12, 2)")
        if amount != amount.quantize(Decimal("0.01")):
            raise ValueError("amount has more than two decimal places")
    except InvalidOperation as exc:
        raise ValueError("amount must be a decimal number") from exc
    result["amount"] = format(amount, ".2f")
    parsed_time = datetime.fromisoformat(row["transaction_time"])
    if parsed_time.tzinfo is not None or not 1970 <= parsed_time.year <= 2105:
        raise ValueError("transaction_time must be a naive timestamp within DateTime range")
    # Dataset has no timezone. Treat its wall clock values as UTC consistently.
    result["transaction_time"] = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
    for field in ("lat", "lon", "merchant_lat", "merchant_lon"):
        value = float(row[field])
        bound = 90 if field.endswith("lat") else 180
        if not math.isfinite(value) or not -bound <= value <= bound:
            raise ValueError(f"invalid coordinate: {field}")
        result[field] = value
    population = int(row["population_city"])
    if not 0 <= population <= 2**32 - 1:
        raise ValueError("population_city must fit UInt32")
    result["population_city"] = population
    target = row.get("target", "")
    if target not in ("", "0", "1"):
        raise ValueError("target must be empty, 0 or 1")
    result["target"] = int(target) if target else None
    result.update(source_file=source_file, source_row=source_row)
    return result


def transactions(path: Path, limit: int = 0):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or set(CSV_FIELDS) - set(reader.fieldnames):
            raise ValueError(f"{path.name}: CSV header does not match the dataset")
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError(f"{path.name}: duplicate column names")
        unexpected = set(reader.fieldnames) - set(CSV_FIELDS) - {"target"}
        if unexpected:
            raise ValueError(f"{path.name}: unexpected columns: {sorted(unexpected)}")
        for number, row in enumerate(reader, start=1):
            if limit and number > limit:
                break
            try:
                yield normalize(row, path.name, number)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"{path.name}, data row {number}: {exc}") from exc


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def query(sql: str, query_id: str | None = None) -> str:
    params = {"wait_end_of_query": "1"}
    if query_id:
        params["query_id"] = query_id
    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
    credentials = (os.getenv("CLICKHOUSE_USER", "homework") + ":" +
                   os.getenv("CLICKHOUSE_PASSWORD", "homework"))
    request = urllib.request.Request(
        url + "/?" + urllib.parse.urlencode(params),
        data=sql.encode(),
        headers={"Authorization": "Basic " + base64.b64encode(credentials.encode()).decode()},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"ClickHouse HTTP {exc.code}: {exc.read().decode()}") from exc


def query_json(sql: str):
    return [json.loads(line) for line in query(sql.rstrip("; \n") + " FORMAT JSONEachRow").splitlines()]


def csv_result(text: str) -> list[dict]:
    return [
        {"us_state": row["us_state"], "cat_id": row["cat_id"],
         "max_amount": format(Decimal(row["max_amount"]), ".2f")}
        for row in csv.DictReader(text.splitlines())
    ]
