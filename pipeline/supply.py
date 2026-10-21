"""Monthly data supply with a transactional run ledger and bounded history."""
import calendar
import json
import os
import random
from datetime import datetime

from pipeline.storage import add_month, db, run_token


def append_month(run_key):
    """Read existing labeled rows; append a deterministic sample into the next month.

    Batch and run ledger commit together. Retrying a committed run returns its month,
    so a task failure cannot accidentally append two months. Days are calendar-clamped.
    """
    batch_size = int(os.getenv("MONTHLY_BATCH_SIZE", "10000"))
    if batch_size < 2:
        raise ValueError("MONTHLY_BATCH_SIZE must be >= 2")
    with db(write=True) as conn:
        existing = conn.execute("SELECT month FROM batches WHERE run_key=?", (run_key,)).fetchone()
        if existing:
            return existing["month"]
        rows = conn.execute("SELECT transaction_time,payload,target FROM labeled_transactions ORDER BY transaction_id").fetchall()
        if not rows:
            raise ValueError("warehouse is empty")
        month = add_month(max(row["transaction_time"][:7] for row in rows))
        pointer = conn.execute("SELECT value FROM state WHERE key='trained_month'").fetchone()
        # Backpressure keeps a stalled/rejected training month inside the retention horizon.
        if pointer and month > add_month(pointer["value"], 2):
            return None
        rng = random.Random(run_token(run_key))
        sampled = rng.choices(rows, k=batch_size)
        # Preserve both classes for the demonstration even with a rare fraud label.
        for label in (0, 1):
            matches = [row for row in rows if row["target"] == label]
            if not matches:
                raise ValueError("source requires both classes")
            sampled[label] = rng.choice(matches)
        year, number = map(int, month.split("-"))
        records = []
        for index, row in enumerate(sampled):
            record = json.loads(row["payload"])
            old = datetime.fromisoformat(row["transaction_time"])
            timestamp = old.replace(year=year, month=number, day=min(old.day, calendar.monthrange(year, number)[1]))
            record["transaction_time"] = timestamp.isoformat()
            records.append((f"batch-{run_token(run_key)}-{index}", timestamp.isoformat(), json.dumps(record), row["target"]))
        conn.executemany("INSERT INTO labeled_transactions VALUES (?,?,?,?)", records)
        conn.execute("INSERT INTO batches VALUES (?,?,?)", (run_key, month, len(records)))
        return month


def prune_history(month):
    """Delete rows outside 36 months, protecting the next untrained three-month window.

    Repeating DELETE uses the same cutoff and does not move supply/training pointers.
    A separate task makes the append → retention dependency visible in Airflow.
    """
    if month is None:
        return
    history = int(os.getenv("HISTORY_MONTHS", "36"))
    if history < 3:
        raise ValueError("HISTORY_MONTHS must be >= 3")
    cutoff = add_month(month, -(history - 1))
    with db(write=True) as conn:
        pointer = conn.execute("SELECT value FROM state WHERE key='trained_month'").fetchone()
        if pointer:
            cutoff = min(cutoff, add_month(pointer["value"], -1))
        conn.execute("DELETE FROM labeled_transactions WHERE transaction_time < ?", (cutoff + "-01",))
    return cutoff
