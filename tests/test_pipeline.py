import json
import io
import math
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from scripts.export_preprocessing import export
from app.settings import MODEL_PATH, PREPROCESSING_PATH
from src.adapters.csv_input import read_transactions
from src.adapters.messages import parse_score, parse_transaction, score_message, transaction_message
from src.application.use_cases import GetResults
from src.domain.entities import ResultsSnapshot, SavedScore, ScoreResult
from src.infrastructure.ml.preprocessing import Preprocessor, distance_km
from app.bootstrap import build_scorer
from src.infrastructure.kafka import commit_message

from tests.fixtures import transaction_csv

FIXTURE_CSV = transaction_csv()


@pytest.fixture
def record():
    return read_transactions(FIXTURE_CSV)[0]


def test_real_model_changes_with_input_and_respects_threshold(record):
    scorer = build_scorer()
    a = scorer.execute(record)
    b = scorer.execute(replace(record, amount=100000))
    assert a.score != b.score
    assert 0 <= a.score <= 1
    assert set(score_message(a)) == {"transaction_id", "score", "fraud_flag"}
    assert build_scorer(threshold=0).execute(record).fraud_flag == 1
    assert build_scorer(threshold=1).execute(record).fraud_flag == 0


def test_features_match_model_and_known_geography(record):
    features = Preprocessor(PREPROCESSING_PATH).transform(record)
    assert features.columns.tolist() == build_scorer().predictor.model.feature_names_
    assert features.hour.iloc[0] == 12
    assert features.day_of_week.iloc[0] == 6
    assert features.amount_log.iloc[0] == pytest.approx(math.log1p(12))
    assert distance_km(replace(record, lat=0, lon=0, merchant_lat=0, merchant_lon=1)) == pytest.approx(111.195, abs=0.001)


@pytest.mark.parametrize("field,value", [("amount", -1), ("amount", "inf"), ("lat", 91), ("lon", None),
                                          ("transaction_time", "bad"), ("transaction_id", "")])
def test_invalid_transaction_is_rejected(record, field, value):
    with pytest.raises(ValueError):
        build_scorer().execute(parse_transaction({**transaction_message(record), field: value}))


def test_unknown_categories_and_missing_numeric_values(record):
    features = Preprocessor(PREPROCESSING_PATH).transform(replace(record, jobs="new unseen job", amount=None))
    assert features.jobs_cat.iloc[0] == "cat_NAN"
    means = json.loads(PREPROCESSING_PATH.read_text())["numeric_means"]
    assert features.amount_log.iloc[0] == pytest.approx(math.log1p(means["amount"]))
    assert pd.notna(features).all().all()


def test_csv_without_ids_has_stable_distinct_ids():
    frame = pd.read_csv(io.BytesIO(FIXTURE_CSV)).drop(columns="transaction_id")
    content = frame.to_csv(index=False).encode()
    one, two = read_transactions(content), read_transactions(content)
    assert one == two
    assert len({r.transaction_id for r in one}) == len(one)


def test_row_limit_preserves_full_file_transaction_ids():
    frame = pd.read_csv(io.BytesIO(FIXTURE_CSV)).drop(columns="transaction_id")
    content = frame.to_csv(index=False).encode()
    assert read_transactions(content, limit=2) == read_transactions(content)[:2]


def test_csv_rejects_ids_that_collide_after_whitespace_normalization():
    frame = pd.read_csv(io.BytesIO(FIXTURE_CSV))
    frame.loc[0, "transaction_id"] = "same-id"
    frame.loc[1, "transaction_id"] = " same-id "
    with pytest.raises(ValueError, match="duplicate transaction_id"):
        read_transactions(frame.to_csv(index=False).encode())


@pytest.mark.parametrize("score,flag", [(float("nan"), 0), (1.1, 1), (-0.1, 0), (True, 1), (0.5, True), (0.5, "1")])
def test_invalid_scores(score, flag):
    with pytest.raises(ValueError):
        parse_score({"transaction_id": "test", "score": score, "fraud_flag": flag})


def test_artifact_export_preserves_frequency_and_target_means(tmp_path):
    frame = pd.DataFrame([transaction_message(tx) for tx in read_transactions(FIXTURE_CSV)])
    frame["target"] = [0, 1, 0, 1, 0]
    train = tmp_path / "train.csv"
    output = tmp_path / "preprocessing.json"
    frame.to_csv(train, index=False)
    export(train, MODEL_PATH, output)
    artifact = json.loads(output.read_text())
    assert artifact["mode"] == "competition"
    assert artifact["global_mean"] == pytest.approx(0.4)
    assert artifact["categories"]["gender"] == {"M": "cat_0", "F": "cat_1"}
    assert artifact["target_means"]["gender_cat"] == {"cat_0": 0.0, "cat_1": 1.0}
    features = Preprocessor(output).transform(read_transactions(FIXTURE_CSV)[0])
    assert features.gender_cat_mean_enc.iloc[0] == 0
    assert features.amount_log.iloc[0] == pytest.approx(math.log1p(12))
    assert 0 <= build_scorer(preprocessing_path=output).execute(read_transactions(FIXTURE_CSV)[0]).score <= 1


def test_commit_advances_only_processed_partition():
    from types import SimpleNamespace
    class Consumer:
        def commit(self, offsets):
            self.offsets = offsets
    consumer = Consumer()
    commit_message(consumer, SimpleNamespace(topic="transactions", partition=2, offset=41))
    assert len(consumer.offsets) == 1
    (partition, offset), = consumer.offsets.items()
    assert partition.partition == 2 and offset.offset == 42


def test_ui_empty_results(monkeypatch):
    from app import bootstrap
    class Reader:
        def read(self, fraud_limit, scores_limit):
            return ResultsSnapshot((), (), 0)
    monkeypatch.setattr(bootstrap, "test_csv", transaction_csv)
    monkeypatch.setattr(bootstrap, "build_get_results", lambda: GetResults(Reader()))
    app = AppTest.from_file("app/ui.py").run()
    assert not app.exception
    app.button[0].click().run()
    assert not app.exception
    assert app.metric[0].value == "0"
    assert any("fraud_flag = 1" in info.value for info in app.info)


def test_ui_populated_results(monkeypatch):
    from app import bootstrap
    class Reader:
        def read(self, fraud_limit, scores_limit):
            saved = SavedScore(ScoreResult("x", 0.99, 1), datetime(2023, 1, 1))
            return ResultsSnapshot((saved,), (0.01, 0.5, 0.99), 3)
    monkeypatch.setattr(bootstrap, "test_csv", transaction_csv)
    monkeypatch.setattr(bootstrap, "build_get_results", lambda: GetResults(Reader()))
    app = AppTest.from_file("app/ui.py").run()
    app.button[0].click().run()
    assert not app.exception
    assert app.metric[0].value == "3"
    assert len(app.dataframe[0].value) == 1
    assert len(app.get("arrow_vega_lite_chart")) == 1
