"""Feed original transaction fields into Kafka continuously, without target leakage."""
import json
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer
from pipeline.storage import db


def main():
    with db() as conn:
        rows = conn.execute("SELECT payload FROM labeled_transactions LIMIT 10000").fetchall()
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), "enable.idempotence": True})
    while True:
        record = json.loads(random.choice(rows)["payload"])
        record.pop("target", None)
        record["transaction_id"] = str(uuid.uuid4())
        record["transaction_time"] = datetime.utcnow().isoformat()
        failures = []
        producer.produce("transactions", json.dumps(record).encode(), on_delivery=lambda err, msg: failures.append(str(err)) if err else None)
        if producer.flush(30) or failures:
            raise RuntimeError(str(failures))
        Path("/tmp/simulator-heartbeat").touch()
        time.sleep(float(os.getenv("SIMULATOR_INTERVAL", "0.5")))


if __name__ == "__main__":
    main()
