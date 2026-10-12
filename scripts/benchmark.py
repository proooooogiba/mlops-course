"""Measure baseline/optimized storage and raw/aggregate query work."""

import argparse
import json
import statistics
import uuid
from pathlib import Path

from scripts.common import SQL_DIR, csv_result, query, query_json, write_json
from scripts.verify import wait_for_ingestion


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--results-dir", type=Path, default=Path("/results/runtime"))
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    manifest = json.loads((args.results_dir / "load_manifest.json").read_text())
    wait_for_ingestion(manifest, timeout=300)
    # No ingestion should be running during the benchmark. This snapshot is repeatable.
    query((SQL_DIR / "00_baseline.sql").read_text())
    query("TRUNCATE TABLE homework.transactions_baseline")
    query("INSERT INTO homework.transactions_baseline SELECT * FROM homework.transactions")
    # Normalize part counts for a fairer storage comparison; not needed by normal queries.
    for table in ("transactions_baseline", "transactions", "state_max"):
        query(f"OPTIMIZE TABLE homework.{table} FINAL")

    raw_sql = (SQL_DIR / "02_max_transaction_by_state.sql").read_text()
    statements = {
        "baseline": raw_sql.replace("FROM homework.transactions\n", "FROM homework.transactions_baseline\n"),
        "optimized_raw": raw_sql,
        "aggregate": (SQL_DIR / "03_max_transaction_by_state_optimized.sql").read_text(),
    }
    reference = None
    samples = {}
    for label, sql in statements.items():
        result = csv_result(query(sql))  # warm-up; query cache is disabled by default
        if reference is None:
            reference = result
        if result != reference:
            raise AssertionError(f"{label} disagrees with the baseline")
        ids = []
        for _ in range(args.runs):
            query_id = f"hw2-{label}-{uuid.uuid4()}"
            if csv_result(query(sql, query_id=query_id)) != reference:
                raise AssertionError(f"{label}: result changed while benchmarking")
            ids.append(query_id)
        query("SYSTEM FLUSH LOGS")
        quoted = ",".join(f"'{qid}'" for qid in ids)
        rows = query_json(f"""
            SELECT query_duration_ms, read_rows, read_bytes, memory_usage
            FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query = 1 AND query_id IN ({quoted})
        """)
        if len(rows) != args.runs:
            raise AssertionError(f"Expected {args.runs} query_log entries; got {len(rows)}")
        samples[label] = {
            "runs": rows,
            "median_duration_ms": statistics.median(int(r["query_duration_ms"]) for r in rows),
            "median_read_rows": statistics.median(int(r["read_rows"]) for r in rows),
            "median_read_bytes": statistics.median(int(r["read_bytes"]) for r in rows),
        }
    storage = query_json("""
        SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed_bytes,
               sum(data_uncompressed_bytes) AS uncompressed_bytes, sum(bytes_on_disk) AS bytes_on_disk
        FROM system.parts WHERE database = 'homework' AND active
        GROUP BY table ORDER BY table
    """)
    report = {"clickhouse_version": query("SELECT version()").strip(),
              "dataset_rows": manifest["total_rows"], "runs_per_query": args.runs,
              "queries": samples, "storage": storage,
              "note": "Warm-cache local single-node measurements; no concurrent producer. Query times vary by host."}
    write_json(args.results_dir / "benchmark.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
