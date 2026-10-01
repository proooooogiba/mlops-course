import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("SCORES_TOPIC", "scores")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead_letters")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@localhost:5432/fraud")
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(ROOT / "models/my_catboost.cbm")))
PREPROCESSING_PATH = Path(os.getenv("PREPROCESSING_PATH", str(ROOT / "models/preprocessing.json")))
TEST_CSV_PATH = Path(os.getenv("TEST_CSV_PATH", str(ROOT / "Teta ML 1 2025/test.csv")))


def fraud_threshold():
    value = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
    if not 0 <= value <= 1:
        raise ValueError("FRAUD_THRESHOLD must be between 0 and 1")
    return value
