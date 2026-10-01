"""Composition root: read settings and connect concrete adapters to use cases."""
import json
from contextlib import contextmanager

import psycopg

from app import settings
from src.application.use_cases import GetResults, ProcessTransaction, ScoreTransaction, StoreScore, SubmitTransactions
from src.domain.entities import FraudPolicy
from src.infrastructure.kafka import KafkaScorePublisher, KafkaTransactionPublisher, Worker, make_producer
from src.infrastructure.ml.catboost import CatBoostPredictor
from src.infrastructure.postgres import PostgresResultsReader, PostgresScoreWriter


def build_scorer(model_path=None, preprocessing_path=None, threshold=None):
    predictor = CatBoostPredictor(model_path or settings.MODEL_PATH, preprocessing_path or settings.PREPROCESSING_PATH)
    policy = FraudPolicy(settings.fraud_threshold() if threshold is None else threshold)
    return ScoreTransaction(predictor, policy)


def build_worker(topic, group):
    return Worker(settings.BOOTSTRAP_SERVERS, topic, group, settings.DLQ_TOPIC)


def build_process_transaction(producer):
    return ProcessTransaction(build_scorer(), KafkaScorePublisher(producer, settings.SCORES_TOPIC))


@contextmanager
def build_store_score():
    with psycopg.connect(settings.DATABASE_URL, connect_timeout=10) as connection:
        yield StoreScore(PostgresScoreWriter(connection))


@contextmanager
def build_submit_transactions():
    producer = make_producer(settings.BOOTSTRAP_SERVERS)
    try:
        yield SubmitTransactions(KafkaTransactionPublisher(producer, settings.TRANSACTIONS_TOPIC))
    finally:
        producer.close(timeout=10)


def build_get_results():
    return GetResults(PostgresResultsReader(settings.DATABASE_URL))


def preprocessing_mode():
    return json.loads(settings.PREPROCESSING_PATH.read_text())["mode"]


def test_csv():
    return settings.TEST_CSV_PATH.read_bytes()
