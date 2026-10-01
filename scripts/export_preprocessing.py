"""Offline export of the original seminar's N+1 / target-mean encodings."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from src.infrastructure.ml.preprocessing import CATEGORIES, TIME_FEATURES, category_key


def export(train_path, model_path, output):
    train = pd.read_csv(train_path)
    if "target" not in train or train.empty or not train.target.isin([0, 1]).all():
        raise ValueError("train.csv must have a nonempty binary target column")
    ts = pd.to_datetime(train["transaction_time"], errors="raise")
    if ts.isna().any():
        raise ValueError("Training timestamps must not be missing")
    for col, values in zip(TIME_FEATURES, (ts.dt.hour, ts.dt.year, ts.dt.month, ts.dt.day, ts.dt.dayofweek)):
        train[col] = values
    categories = {}
    for col in CATEGORIES:
        # Preserve the original pandas ordering, including ties, before assigning ranks.
        counts = train.groupby(col, dropna=False)[["target"]].count().sort_values("target", ascending=False).reset_index()
        mapping = {}
        for rank, value in enumerate(counts[col]):
            mapping[category_key(value)] = "cat_NAN" if pd.isna(value) else f"cat_{rank}" if rank < 50 else "cat_50+"
        categories[col] = mapping
        train[col + "_cat"] = train[col].map(lambda value: mapping[category_key(value)])
    means = {}
    for col in (*[c + "_cat" for c in CATEGORIES], *TIME_FEATURES):
        means[col] = {category_key(key): float(value) for key, value in train.groupby(col).target.mean().items()}
    numeric_means = {}
    for col in ("amount", "population_city"):
        values = pd.to_numeric(train[col], errors="raise")
        value = float(values.mean())
        if pd.isna(value) or value < 0 or (values.dropna() < 0).any():
            raise ValueError(f"Invalid training field: {col}")
        numeric_means[col] = value
    model = CatBoostClassifier()
    model.load_model(str(model_path))
    artifact = {"version": 1, "mode": "competition", "feature_names": model.feature_names_,
                "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
                "training_sha256": hashlib.sha256(train_path.read_bytes()).hexdigest(),
                "global_mean": float(train.target.mean()), "categories": categories,
                "target_means": means, "numeric_means": numeric_means}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    print(f"Exported frozen preprocessing to {output}; model was not trained")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("train", type=Path)
    parser.add_argument("--model", type=Path, default=Path("models/my_catboost.cbm"))
    parser.add_argument("--output", type=Path, default=Path("models/preprocessing.json"))
    args = parser.parse_args()
    export(args.train, args.model, args.output)
