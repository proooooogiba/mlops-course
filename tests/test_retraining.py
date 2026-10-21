"""Behavioral checks for state, failure recovery, data quality and hot reload."""
import json
from datetime import datetime

import pandas as pd
import pytest

from app.scoring_worker import Scorer
from pipeline import retrain
from pipeline.features import features
from pipeline.quality import assert_quality, inspect_dataset, render_report
from pipeline.seed import seed
from pipeline.storage import add_month, db, root
from pipeline.supply import append_month, prune_history


from tests.training_fixtures import demo_records


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    monkeypatch.setenv("SHARED_DIR", str(tmp_path / "shared"))
    monkeypatch.setenv("MONTHLY_BATCH_SIZE", "100")
    monkeypatch.setenv("MODEL_ITERATIONS", "12")
    path = tmp_path / "train.csv"
    pd.DataFrame(demo_records()).to_csv(path, index=False)
    monkeypatch.setenv("TRAIN_DATA_CSV", str(path))
    seed()
    return root()


def pipeline(run_key):
    plan = retrain.prepare_window(run_key)
    return retrain.evaluate_candidate(retrain.train_candidate(plan))


def pointer():
    with db() as conn:
        row = conn.execute("SELECT value FROM state WHERE key='trained_month'").fetchone()
        return row[0] if row else None


def test_temporal_split_and_retry(warehouse):
    import pickle
    plan = retrain.prepare_window("first")
    snapshot = pickle.loads((warehouse / "work" / retrain.run_token("first") / "dataset.pkl").read_bytes())
    assert {r['transaction_time'][:7] for r in snapshot['train']} == {'2020-01', '2020-02'}
    assert {r['transaction_time'][:7] for r in snapshot['test']} == {'2020-03'}
    assert pointer() is None
    seed()
    assert retrain.prepare_window("first")["month"] == plan["month"]
    retrain.publish_model(retrain.evaluate_candidate(retrain.train_candidate(plan)))
    assert pointer() == "2020-03"
    assert retrain.prepare_window("no-new-data") is None
    retrain.publish_model(plan)
    assert pointer() == "2020-03"


def test_supply_retry_calendar_and_backpressure(warehouse):
    retrain.publish_model(pipeline("initial"))
    assert append_month("supply-1") == "2020-04"
    assert append_month("supply-1") == "2020-04"
    assert append_month("supply-2") == "2020-05"
    assert append_month("supply-3") is None
    with db() as conn:
        assert conn.execute("SELECT count(*) FROM labeled_transactions").fetchone()[0] == 500
        dates = [datetime.fromisoformat(r[0]) for r in conn.execute("SELECT transaction_time FROM labeled_transactions")]
        assert len(dates) == 500
    assert add_month("2020-12") == "2021-01"
    prune_history("2020-05")


def test_gate_rejects_and_keeps_pointer_and_model(warehouse):
    retrain.publish_model(pipeline("initial"))
    before = (warehouse / "models/latest.json").read_bytes()
    append_month("month-4")
    plan = pipeline("rejected")
    decision_path = warehouse / "work" / retrain.run_token("rejected") / "decision.json"
    decision = json.loads(decision_path.read_text())
    decision["accepted"] = False
    decision_path.write_text(json.dumps(decision))
    assert retrain.publish_model(plan) is None
    assert (warehouse / "models/latest.json").read_bytes() == before
    assert pointer() == "2020-03"
    assert retrain.prepare_window("retry-new-run")["month"] == "2020-04"
    assert not retrain.passes_gate({"average_precision": .8, "roc_auc": .9}, {"average_precision": .9, "roc_auc": .9})
    assert not retrain.passes_gate({"average_precision": .9, "roc_auc": .8}, {"average_precision": .9, "roc_auc": .9})
    assert retrain.passes_gate({"average_precision": .9, "roc_auc": .9}, {"average_precision": .9, "roc_auc": .9})


def test_manifest_crash_recovers_without_skipping_month(warehouse, monkeypatch):
    plan = pipeline("crash")
    original = retrain.atomic_json
    def fail_after_manifest(path, value):
        original(path, value)
        if str(path).endswith("latest.json"):
            raise RuntimeError("simulated crash after manifest replace")
    monkeypatch.setattr(retrain, "atomic_json", fail_after_manifest)
    with pytest.raises(RuntimeError):
        retrain.publish_model(plan)
    assert pointer() is None
    assert (warehouse / "models/latest.json").exists()
    monkeypatch.setattr(retrain, "atomic_json", original)
    assert retrain.publish_model(plan) == plan["version"]
    assert pointer() == "2020-03"
    assert len(list((warehouse / "models").glob("*.cbm"))) == 1


def test_hot_reload_same_object_and_versioned_score(warehouse):
    first = pipeline("initial")
    retrain.publish_model(first)
    scorer = Scorer()
    record = dict(demo_records()[0], transaction_id="demo")
    result = scorer.score(record)
    assert result["model_version"] == first["version"]
    append_month("month-4")
    next_plan = pipeline("next")
    assert retrain.publish_model(next_plan) is not None
    result = scorer.score(record)
    assert result["model_version"] == next_plan["version"] != first["version"]
    assert 0 <= result["score"] <= 1 and result["fraud_flag"] in (0, 1)
    # Replaying an old published task cannot undo the hot reload.
    retrain.publish_model(first)
    assert scorer.score(record)["model_version"] == next_plan["version"]


def test_dq_failure_still_has_html(warehouse):
    assert_quality(render_report(inspect_dataset("good")))
    with db(write=True) as conn:
        row = conn.execute("SELECT transaction_id,payload FROM labeled_transactions LIMIT 1").fetchone()
        payload = json.loads(row["payload"])
        payload["lat"] = 999
        conn.execute("UPDATE labeled_transactions SET payload=? WHERE transaction_id=?", (json.dumps(payload), row["transaction_id"]))
    path = render_report(inspect_dataset("bad"))
    with pytest.raises(ValueError, match="Coordinate bounds"):
        assert_quality(path)
    assert "FAIL" in (warehouse / "reports/latest.html").read_text()


def test_preprocessing_is_label_free_and_inference_equal():
    record = demo_records()[0]
    expected = features([record])
    without_label = {k: v for k, v in record.items() if k != 'target'}
    pd.testing.assert_frame_equal(expected, features([without_label]))
    assert 'target' not in expected and 'transaction_id' not in expected
    assert {'log_amount', 'distance_km', 'hour_sin', 'hour_cos'} <= set(expected.columns)


def test_retention_protects_next_window(warehouse, monkeypatch):
    monkeypatch.setenv('HISTORY_MONTHS', '3')
    retrain.publish_model(pipeline('initial'))
    append_month('april')
    prune_history('2020-04')
    with db() as conn:
        months = [r[0] for r in conn.execute('SELECT DISTINCT substr(transaction_time,1,7) FROM labeled_transactions ORDER BY 1')]
    assert months == ['2020-02', '2020-03', '2020-04']
    plan = pipeline('april-model')
    assert retrain.publish_model(plan)
    append_month('may')
    prune_history('2020-05')
    with db() as conn:
        months = [r[0] for r in conn.execute('SELECT DISTINCT substr(transaction_time,1,7) FROM labeled_transactions ORDER BY 1')]
    assert months == ['2020-03', '2020-04', '2020-05']


def test_failed_training_does_not_move_window(warehouse, monkeypatch):
    retrain.publish_model(pipeline('initial'))
    append_month('april')
    plan = retrain.prepare_window('failure')
    def fail(*args, **kwargs):
        raise RuntimeError('training process failed')
    monkeypatch.setattr(retrain.CatBoostClassifier, 'fit', fail)
    with pytest.raises(RuntimeError, match='training process failed'):
        retrain.train_candidate(plan)
    assert pointer() == '2020-03'
    assert retrain.prepare_window('failure')['month'] == '2020-04'
    assert retrain.prepare_window('new-attempt')['month'] == '2020-04'
