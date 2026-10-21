"""Initialize a compact labeled warehouse from the original training CSV."""
import json
import os

import pandas as pd

from pipeline.storage import add_month, create_schema, db


def seed():
    """Read train.csv; insert its latest three months once in one transaction.

    A persisted seeded marker makes restarts no-ops; no unlabeled test rows are used.
    """
    with db() as conn:
        create_schema(conn)
        if conn.execute("SELECT 1 FROM state WHERE key='seeded'").fetchone():
            return
    frame = pd.read_csv(os.getenv("TRAIN_DATA_CSV", "/data/train.csv"), dtype={"post_code": str})
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"], errors="raise")
    if frame["target"].isna().any() or not frame["target"].isin([0, 1]).all():
        raise ValueError("train.csv must have binary labels")
    latest = frame.transaction_time.max().strftime("%Y-%m")
    frame = frame[frame.transaction_time >= pd.Timestamp(add_month(latest, -2) + "-01")]
    records = []
    for index, row in frame.iterrows():
        record = row.to_dict()
        record["transaction_time"] = row.transaction_time.isoformat()
        records.append((f"seed-{index}", record["transaction_time"], json.dumps(record), int(record["target"])))
    with db(write=True) as conn:
        if conn.execute("SELECT 1 FROM state WHERE key='seeded'").fetchone():
            return
        conn.executemany("INSERT INTO labeled_transactions VALUES (?, ?, ?, ?)", records)
        conn.execute("INSERT INTO state VALUES ('seeded', ?)", (latest,))
    print(f"Seeded {len(records)} rows through {latest}", flush=True)
