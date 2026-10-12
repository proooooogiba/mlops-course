"""Stream CSV records to Kafka as JSONEachRow; exit only after broker delivery."""

import argparse
import json
import os
from pathlib import Path

from confluent_kafka import Producer

from scripts.common import sha256, transactions, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", type=Path, nargs="+", help="CSV paths (train and/or test)")
    parser.add_argument("--limit", type=int, default=0, help="Rows per file; 0 means all")
    parser.add_argument("--manifest", type=Path, default=Path("/results/runtime/load_manifest.json"))
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    if len({path.name for path in args.files}) != len(args.files):
        parser.error("Input basenames must be unique (they identify CSV sources)")
    for path in args.files:
        if not path.is_file():
            parser.error(f"CSV not found: {path}; copy the Kaggle files into data/ first")
    if args.manifest.exists():
        parser.error("A load manifest already exists. Reset the demo before reloading (see README).")

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "linger.ms": 20,
        "batch.size": 131072,
        "queue.buffering.max.messages": 100000,
        "delivery.timeout.ms": 120000,
    })
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    failures = []
    delivered = 0

    def delivery(error, _message):
        nonlocal delivered
        if error is not None:
            if not failures:
                failures.append(str(error))
        else:
            delivered += 1

    manifest = {"topic": topic, "limit_per_file": args.limit, "files": [], "total_rows": 0}
    for path in args.files:
        digest = sha256(path)
        count = 0
        for record in transactions(path, args.limit):
            if failures:
                raise RuntimeError(f"Kafka delivery failed: {failures[0]}")
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False).encode()
            while True:
                try:
                    producer.produce(topic, key=record["us_state"].encode(), value=payload, on_delivery=delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            count += 1
            if count % 100000 == 0:
                print(f"{path.name}: queued {count:,} rows", flush=True)
        if count == 0:
            raise ValueError(f"{path.name}: CSV has no data rows")
        if sha256(path) != digest:
            raise RuntimeError(f"{path.name} changed during loading")
        manifest["files"].append({"name": path.name, "rows": count, "sha256": digest})
        manifest["total_rows"] += count

    pending = producer.flush(130)
    if pending or failures or delivered != manifest["total_rows"]:
        raise RuntimeError(f"Kafka delivery incomplete: delivered={delivered}, pending={pending}, errors={failures}")
    write_json(args.manifest, manifest)
    print(f"Delivered {delivered:,} rows to {topic}. Manifest: {args.manifest}")


if __name__ == "__main__":
    main()
