"""Exercise real Kafka -> CatBoost -> scores -> PostgreSQL, plus invalid inputs."""
import json
import argparse
import math
import time
import uuid
from pathlib import Path
from dataclasses import replace

import psycopg
from kafka import KafkaConsumer

from app.settings import BOOTSTRAP_SERVERS, DATABASE_URL, DLQ_TOPIC, SCORES_TOPIC, TRANSACTIONS_TOPIC, TEST_CSV_PATH
from src.adapters.csv_input import read_transactions
from app.bootstrap import build_scorer
from src.adapters.messages import score_message, transaction_message
from src.infrastructure.kafka import make_producer, publish


def await_messages(consumer, predicate, count, timeout=45):
    found = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for batch in consumer.poll(timeout_ms=500).values():
            for message in batch:
                value = json.loads(message.value)
                if predicate(value):
                    found.append(value)
        if len(found) >= count:
            return found
    raise AssertionError(f"Expected {count} matching Kafka messages, received {len(found)}")


def main(csv_path=TEST_CSV_PATH, limit=100):
    run_id = "smoke-" + uuid.uuid4().hex
    records = read_transactions(csv_path.read_bytes(), limit=limit)
    if len(records) < 2:
        raise ValueError("Smoke test requires at least two transactions")
    records = [replace(record, transaction_id=f"{run_id}-{idx}") for idx, record in enumerate(records)]
    scorer = build_scorer()
    expected = {r.transaction_id: score_message(scorer.execute(r)) for r in records}
    # A new group reads from earliest, filtering by unique IDs, so startup timing cannot lose results.
    consumer = KafkaConsumer(SCORES_TOPIC, DLQ_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS,
                             group_id=run_id, auto_offset_reset="earliest", enable_auto_commit=False)
    producer = make_producer(BOOTSTRAP_SERVERS)
    try:
        for record in records:
            publish(producer, TRANSACTIONS_TOPIC, transaction_message(record))
        scored = await_messages(consumer, lambda r: r.get("transaction_id") in expected, len(records))
        for result in scored:
            assert set(result) == {"transaction_id", "score", "fraud_flag"}, result
            assert math.isclose(result["score"], expected[result["transaction_id"]]["score"], rel_tol=1e-8), result
            assert result["fraud_flag"] == expected[result["transaction_id"]]["fraud_flag"], result
        ids = list(expected)
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                rows = conn.execute("SELECT transaction_id, score, fraud_flag FROM transaction_scores WHERE transaction_id = ANY(%s)", (ids,)).fetchall()
                if len(rows) == len(records):
                    break
                time.sleep(0.5)
            assert len(rows) == len(records), "Not all model results reached PostgreSQL"
            for tx_id, score, flag in rows:
                assert math.isclose(score, expected[tx_id]["score"], rel_tol=1e-8)
                assert flag == expected[tx_id]["fraud_flag"]
            for record in records:
                publish(producer, TRANSACTIONS_TOPIC, transaction_message(record))
            await_messages(consumer, lambda r: r.get("transaction_id") in expected, len(records))
            # Barrier record confirms the sink has processed all preceding duplicate scores.
            barrier = {**transaction_message(records[0]), "transaction_id": run_id + "-barrier"}
            publish(producer, TRANSACTIONS_TOPIC, barrier)
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if conn.execute("SELECT 1 FROM transaction_scores WHERE transaction_id = %s", (barrier["transaction_id"],)).fetchone():
                    break
                time.sleep(0.5)
            else:
                raise AssertionError("Duplicate-delivery barrier was not stored")
            count = conn.execute("SELECT COUNT(*) FROM transaction_scores WHERE transaction_id = ANY(%s)", (ids,)).fetchone()[0]
            assert count == len(records), "Duplicate delivery created duplicate rows"
        bad = publish_invalid(producer, TRANSACTIONS_TOPIC, {"transaction_id": run_id + "-invalid"})
        errors = await_messages(consumer, lambda r: r.get("source_topic") == TRANSACTIONS_TOPIC and r.get("offset") == bad.offset, 1)
        assert errors[0]["stage"] == "scorer"
        malformed_score = publish_invalid(producer, SCORES_TOPIC, {"transaction_id": run_id + "-bad-score", "score": 2.0, "fraud_flag": 1})
        errors = await_messages(consumer, lambda r: r.get("source_topic") == SCORES_TOPIC and r.get("offset") == malformed_score.offset, 1)
        assert errors[0]["stage"] == "postgres-sink"
        # Ensure scoring continues after a poison message.
        after = {**transaction_message(records[1]), "transaction_id": run_id + "-after-invalid"}
        publish(producer, TRANSACTIONS_TOPIC, after)
        await_messages(consumer, lambda r: r.get("transaction_id") == after["transaction_id"], 1)
        print("PASS: real CPU inference, exact scores schema, PostgreSQL writes, deduplication, dead letters, continued processing")
    finally:
        consumer.close(autocommit=False)
        producer.close(timeout=10)


def publish_invalid(producer, topic, record):
    return producer.send(topic, key=record["transaction_id"], value=record).get(timeout=30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=TEST_CSV_PATH)
    parser.add_argument("--limit", type=int, default=100, help="Number of real CSV rows to test")
    args = parser.parse_args()
    if args.limit < 2:
        parser.error("--limit must be at least 2")
    main(args.csv, args.limit)
