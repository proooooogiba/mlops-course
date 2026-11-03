"""Kafka consumer with atomic model hot reload and versioned results."""
import json
import logging
import math
import os
import time
from pathlib import Path
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from app.monitoring import ScoringMetrics, serve
from pipeline.features import features
from pipeline.retrain import load_model
from pipeline.storage import db, now, root


class Scorer:
    def __init__(self, metrics=None):
        self.metrics = metrics
        self.version = None
        self.model = None

    def score(self, record, fallback_id=None):
        manifest = json.loads((root() / "models/latest.json").read_text())
        if self.version != manifest["version"]:
            # A model object and its version are swapped together only after a full load.
            model = load_model(root() / "models" / manifest["file"])
            previous = self.version
            self.model, self.version = model, manifest["version"]
            if self.metrics is not None:
                self.metrics.model_loaded(self.version, previous)
            logging.info("Loaded model %s", self.version)
        score = float(self.model.predict_proba(features([record]))[0, 1])
        amount = float(record["amount"])
        if not math.isfinite(amount) or amount < 0:
            raise ValueError("amount must be finite and nonnegative")
        return {"transaction_id": str(record.get("transaction_id", fallback_id)),
                "score": score, "fraud_flag": int(score > float(os.getenv("FRAUD_THRESHOLD", "0.98"))),
                "model_version": self.version,
                "transaction_time": record["transaction_time"],
                "scored_at": datetime.now(timezone.utc).isoformat(),
                "us_state": str(record["us_state"]), "merch": str(record["merch"]),
                "cat_id": str(record["cat_id"]), "amount": amount}


def main():
    logging.basicConfig(level=logging.INFO)
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = Consumer({"bootstrap.servers": broker, "group.id": "fraud-scorer-hw3",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True})
    consumer.subscribe(["transactions"])
    metrics = ScoringMetrics()
    serve(metrics, int(os.getenv("METRICS_PORT", "8000")))
    scorer = Scorer(metrics)
    try:
        while True:
            message = consumer.poll(1)
            Path("/tmp/scorer-heartbeat").touch()
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            fallback_id = f"{message.topic()}:{message.partition()}:{message.offset()}"
            started = time.perf_counter()
            try:
                record = json.loads(message.value())
                result = scorer.score(record, fallback_id)
            except (ValueError, KeyError, TypeError) as exc:
                result = {"source": fallback_id, "error": str(exc)}
                topic = "dead_letters"
                metrics.errors.inc()
            else:
                topic = "scores"
                with db(write=True) as conn:
                    conn.execute("INSERT INTO scores VALUES (?,?,?,?,?) ON CONFLICT(transaction_id) DO NOTHING",
                                 (result["transaction_id"], result["score"], result["fraud_flag"], result["model_version"], now()))
            errors = []
            producer.produce(topic, json.dumps(result).encode(), on_delivery=lambda err, msg: errors.append(str(err)) if err else None)
            if producer.flush(30) or errors:
                raise RuntimeError(f"Kafka delivery failed: {errors}")
            consumer.commit(message=message, asynchronous=False)
            if topic == "scores":
                metrics.processed(record, result, time.perf_counter() - started)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
