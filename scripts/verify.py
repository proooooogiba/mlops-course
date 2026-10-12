"""Wait for ingestion, compare both SQL queries with CSV, and export the result."""

import argparse
import csv
import json
import time
from decimal import Decimal
from pathlib import Path

from scripts.common import SQL_DIR, csv_result, query, query_json, sha256, transactions, write_json


def expected_result(data_dir: Path, manifest: dict) -> list[dict]:
    winners = {}
    for source in manifest["files"]:
        path = data_dir / source["name"]
        if sha256(path) != source["sha256"]:
            raise ValueError(f"{path.name}: CSV checksum differs from the published file")
        count = 0
        for record in transactions(path, manifest["limit_per_file"]):
            count += 1
            key = (Decimal(record["amount"]), record["source_file"], record["source_row"])
            state = record["us_state"]
            if state not in winners or key > winners[state][0]:
                winners[state] = (key, record["cat_id"])
        if count != source["rows"]:
            raise AssertionError(f"{path.name}: manifest row count differs from CSV")
    return [
        {"us_state": state, "cat_id": winners[state][1],
         "max_amount": format(winners[state][0][0], ".2f")}
        for state in sorted(winners)
    ]


def wait_for_ingestion(manifest: dict, timeout: int):
    deadline = time.monotonic() + timeout
    expected = {s["name"]: s["rows"] for s in manifest["files"]}
    while True:
        counts = query_json("""
            SELECT source_file, count() AS rows, uniqExact(source_row) AS unique_rows,
                   min(source_row) AS first_row, max(source_row) AS last_row
            FROM homework.transactions GROUP BY source_file
        """)
        actual = {r["source_file"]: int(r["rows"]) for r in counts}
        if set(actual) - set(expected) or any(actual[k] > expected[k] for k in actual if k in expected):
            raise AssertionError(f"Unexpected sources or duplicate rows: {actual}; expected {expected}. Reset before reloading.")
        if actual == expected:
            for row in counts:
                total = expected[row["source_file"]]
                if (int(row["unique_rows"]), int(row["first_row"]), int(row["last_row"])) != (total, 1, total):
                    raise AssertionError(f"Missing/duplicate source row numbers: {row}")
            return counts
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Ingestion timeout: {actual}; expected {expected}. Check ClickHouse/Kafka logs.")
        print(f"Waiting for ClickHouse: {sum(actual.values()):,}/{manifest['total_rows']:,}", flush=True)
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--results-dir", type=Path, default=Path("/results/runtime"))
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    manifest = json.loads((args.results_dir / "load_manifest.json").read_text())
    counts = wait_for_ingestion(manifest, args.timeout)
    expected = expected_result(args.data_dir, manifest)
    raw = csv_result(query((SQL_DIR / "02_max_transaction_by_state.sql").read_text()))
    optimized = csv_result(query((SQL_DIR / "03_max_transaction_by_state_optimized.sql").read_text()))
    if raw != expected or optimized != expected:
        raise AssertionError("CSV oracle, raw SQL and optimized SQL returned different results")
    output = args.results_dir / "max_transaction_by_state.csv"
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["us_state", "cat_id", "max_amount"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(optimized)
    report = {"status": "passed", "total_rows": manifest["total_rows"],
              "states": len(expected), "source_counts": counts,
              "checks": ["source checksums", "row counts", "unique row numbers",
                         "CSV oracle == raw SQL == optimized SQL"]}
    write_json(args.results_dir / "verification.json", report)
    print(f"PASS: {manifest['total_rows']:,} rows; {len(expected)} states; CSV == raw SQL == optimized SQL")
    print(f"Result: {output}")


if __name__ == "__main__":
    main()
