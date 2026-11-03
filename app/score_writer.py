"""Kafka scores → PostgreSQL, with commit-before-ack and idempotent inserts."""
import json
import logging
import os
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer

from app.monitoring import WriterMetrics, serve

INSERT_SCORE = '''
    INSERT INTO scores (transaction_id, score, fraud_flag, model_version,
                        transaction_time, scored_at, us_state, merch, cat_id, amount)
    VALUES (%(transaction_id)s, %(score)s, %(fraud_flag)s, %(model_version)s,
            %(transaction_time)s, %(scored_at)s, %(us_state)s, %(merch)s, %(cat_id)s, %(amount)s)
    ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id
'''


def write_score(conn, result):
    # The context commits PostgreSQL before the caller can acknowledge Kafka.
    values = {**result, 'scored_at': result.get('scored_at', result['transaction_time'])}
    with conn.transaction():
        return conn.execute(INSERT_SCORE, values).fetchone() is not None


def main():
    logging.basicConfig(level=logging.INFO)
    metrics = WriterMetrics()
    serve(metrics, int(os.getenv('METRICS_PORT', '8001')))
    consumer = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                         'group.id': 'fraud-postgres-writer-hw5', 'auto.offset.reset': 'earliest',
                         'enable.auto.commit': False})
    consumer.subscribe(['scores'])
    conn = psycopg.connect(os.environ['SCORES_DSN'], autocommit=True)
    try:
        while True:
            msg = consumer.poll(1)
            Path('/tmp/writer-heartbeat').touch()
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            result = json.loads(msg.value())
            started = time.perf_counter()
            try:
                inserted = write_score(conn, result)
            except psycopg.Error:
                metrics.errors.inc()
                logging.exception('PostgreSQL write failed; offset remains uncommitted')
                raise
            metrics.committed(result, time.perf_counter() - started, inserted)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    main()
