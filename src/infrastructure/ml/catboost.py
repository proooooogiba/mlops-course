"""Load a pretrained CatBoost model once and return its positive-class score."""
import hashlib
import logging
from pathlib import Path

from catboost import CatBoostClassifier

from src.domain.entities import Transaction
from src.infrastructure.ml.preprocessing import Preprocessor


class CatBoostPredictor:
    def __init__(self, model_path, preprocessing_path):
        self.preprocessor = Preprocessor(preprocessing_path)
        self.model = CatBoostClassifier()
        self.model.load_model(str(model_path))
        if self.model.feature_names_ != self.preprocessor.artifact["feature_names"]:
            raise ValueError("Model and preprocessing feature schemas differ")
        if hashlib.sha256(Path(model_path).read_bytes()).hexdigest() != self.preprocessor.artifact["model_sha256"]:
            raise ValueError("Preprocessing artifact belongs to a different model")
        if self.preprocessor.artifact["mode"] == "demo":
            logging.warning("DEMO encodings: scoring is real, but quality on competition data is not validated")

    def predict(self, transaction: Transaction) -> float:
        features = self.preprocessor.transform(transaction)
        return float(self.model.predict_proba(features, thread_count=1, task_type="CPU")[0, 1])
