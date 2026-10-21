"""SQLite warehouse and atomic files shared by Airflow and scoring."""
import hashlib
import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


def root():
    return Path(os.getenv("SHARED_DIR", "/shared"))


def initialize_dirs():
    for name in ("warehouse", "models", "reports", "work"):
        (root() / name).mkdir(parents=True, exist_ok=True)


@contextmanager
def db(write=False):
    initialize_dirs()
    conn = sqlite3.connect(root() / "warehouse/fraud.db", timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=120000")
        if write:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def atomic_bytes(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def atomic_json(path, value):
    atomic_bytes(path, json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode())


def run_token(run_key):
    return hashlib.sha256(run_key.encode()).hexdigest()[:20]


def add_month(month, delta=1):
    year, number = map(int, month[:7].split("-"))
    index = year * 12 + number - 1 + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def create_schema(conn):
    conn.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS labeled_transactions (
        transaction_id TEXT PRIMARY KEY, transaction_time TEXT NOT NULL,
        payload TEXT NOT NULL, target INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS labeled_time ON labeled_transactions(transaction_time);
    CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS batches (run_key TEXT PRIMARY KEY, month TEXT UNIQUE, rows INTEGER);
    CREATE TABLE IF NOT EXISTS training_runs (
        run_key TEXT PRIMARY KEY, month TEXT NOT NULL, window_start TEXT NOT NULL,
        status TEXT NOT NULL, version TEXT NOT NULL, metrics TEXT);
    CREATE TABLE IF NOT EXISTS scores (
        transaction_id TEXT PRIMARY KEY, score REAL NOT NULL, fraud_flag INTEGER NOT NULL,
        model_version TEXT NOT NULL, received_at TEXT NOT NULL);
    """)


def now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
