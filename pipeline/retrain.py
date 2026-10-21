"""Temporal training, champion comparison and crash-recoverable publication."""
import json
import os
import pickle
import tempfile
from pathlib import Path

from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

from pipeline.features import CATEGORICAL, features
from pipeline.storage import add_month, atomic_bytes, atomic_json, db, root, run_token


def prepare_window(run_key):
    """Snapshot the next three consecutive months into a run-specific artifact.

    The run ledger fixes the month before any training. An existing snapshot is reused
    on retries; the global trained_month pointer is never advanced by preparation.
    """
    token = run_token(run_key)
    path = root() / "work" / token
    path.mkdir(parents=True, exist_ok=True)
    with db(write=True) as conn:
        run = conn.execute("SELECT * FROM training_runs WHERE run_key=?", (run_key,)).fetchone()
        if run is None:
            pointer = conn.execute("SELECT value FROM state WHERE key='trained_month'").fetchone()
            months = [r[0] for r in conn.execute("SELECT DISTINCT substr(transaction_time,1,7) FROM labeled_transactions ORDER BY 1")]
            month = add_month(pointer["value"]) if pointer else months[-1]
            start = add_month(month, -2)
            if any(add_month(start, i) not in months for i in range(3)):
                return None
            conn.execute("INSERT INTO training_runs VALUES (?,?,?,?,?,NULL)",
                         (run_key, month, start, "prepared", "v_" + token,))
            run = conn.execute("SELECT * FROM training_runs WHERE run_key=?", (run_key,)).fetchone()
        if (path / "dataset.pkl").exists():
            return {**dict(run), "path": str(path)}
        rows = conn.execute("SELECT transaction_time,payload,target FROM labeled_transactions WHERE transaction_time>=? AND transaction_time<? ORDER BY transaction_time,transaction_id",
                            (run["window_start"] + "-01", add_month(run["month"]) + "-01")).fetchall()
        train, test = [], []
        for row in rows:
            record = json.loads(row["payload"])
            (test if row["transaction_time"][:7] == run["month"] else train).append(record)
        for split in (train, test):
            if {r["target"] for r in split} != {0, 1}:
                raise ValueError("each temporal split must contain both classes")
        atomic_bytes(path / "dataset.pkl", pickle.dumps({"train": train, "test": test}))
    return {**dict(run), "path": str(path)}


def train_candidate(plan):
    """Read the frozen temporal train split; write a CPU CatBoost candidate.

    Fixed seed and fixed iterations make retries reproducible. Test data is never used
    for fitting or early stopping; the candidate file is replaced atomically.
    """
    if plan is None:
        return None
    path = Path(plan["path"])
    data = pickle.loads((path / "dataset.pkl").read_bytes())
    model = CatBoostClassifier(iterations=int(os.getenv("MODEL_ITERATIONS", "80")), depth=5,
                               learning_rate=0.08, loss_function="Logloss", random_seed=42,
                               thread_count=2, verbose=False, allow_writing_files=False)
    model.fit(features(data["train"]), [r["target"] for r in data["train"]], cat_features=CATEGORICAL)
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "candidate.cbm"
        model.save_model(str(target))
        atomic_bytes(path / "candidate.cbm", target.read_bytes())
    return plan


def load_model(path):
    model = CatBoostClassifier()
    model.load_model(str(path))
    return model


def metrics(model, records):
    predicted = model.predict_proba(features(records))[:, 1]
    labels = [r["target"] for r in records]
    return {"average_precision": float(average_precision_score(labels, predicted)),
            "roc_auc": float(roc_auc_score(labels, predicted))}


def passes_gate(candidate, champion):
    return champion is None or all(candidate[key] >= champion[key] for key in ("average_precision", "roc_auc"))


def evaluate_candidate(plan):
    """Score candidate and current champion on the identical newest-month snapshot.

    Write a run-specific decision report; retries recompute it without moving state.
    Both AP and ROC-AUC must be nondecreasing (ties pass); lower metrics block rollout.
    """
    if plan is None:
        return None
    path = Path(plan["path"])
    data = pickle.loads((path / "dataset.pkl").read_bytes())
    candidate = metrics(load_model(path / "candidate.cbm"), data["test"])
    latest = root() / "models/latest.json"
    champion, champion_version = None, None
    if latest.exists():
        manifest = json.loads(latest.read_text())
        champion_version = manifest["version"]
        champion = metrics(load_model(root() / "models" / manifest["file"]), data["test"])
    decision = {"version": plan["version"], "month": plan["month"], "window_start": plan["window_start"],
                "train_rows": len(data["train"]), "test_rows": len(data["test"]),
                "candidate": candidate, "champion": champion, "champion_version": champion_version,
                "accepted": passes_gate(candidate, champion)}
    atomic_json(path / "decision.json", decision)
    atomic_json(root() / "reports" / f"training_{plan['version']}.json", decision)
    return plan


def publish_model(plan):
    """Publish an accepted immutable binary, then atomically switch latest.json.

    Only after both files are saved does trained_month advance in SQLite. A retry
    after a crash between manifest and DB commit completes the same month/version.
    Replaying an already published run cannot roll the champion backwards.
    """
    if plan is None:
        return None
    path = Path(plan["path"])
    decision = json.loads((path / "decision.json").read_text())
    with db(write=True) as conn:
        run = conn.execute("SELECT * FROM training_runs WHERE run_key=?", (plan["run_key"],)).fetchone()
        if run["status"] == "published":
            return plan["version"]
        if not decision["accepted"]:
            conn.execute("UPDATE training_runs SET status='rejected',metrics=? WHERE run_key=?",
                         (json.dumps(decision), plan["run_key"]))
            return None
        latest = root() / "models/latest.json"
        current = json.loads(latest.read_text()) if latest.exists() else None
        # A changed champion requires reevaluation, except recovery of our own manifest.
        if current and current["version"] not in (decision["champion_version"], plan["version"]):
            raise RuntimeError("champion changed: rerun evaluate_candidate before publication")
        filename = f"model_{plan['version']}.cbm"
        binary = root() / "models" / filename
        if not binary.exists():
            atomic_bytes(binary, (path / "candidate.cbm").read_bytes())
        atomic_json(latest, {"version": plan["version"], "file": filename, "month": plan["month"],
                             "metrics": decision["candidate"], "window_start": plan["window_start"]})
        conn.execute("INSERT INTO state VALUES ('trained_month',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (plan["month"],))
        conn.execute("UPDATE training_runs SET status='published',metrics=? WHERE run_key=?",
                     (json.dumps(decision), plan["run_key"]))
    return plan["version"]
