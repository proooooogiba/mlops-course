"""Transform one transaction using frozen encodings; no fitting during inference."""
import json
import math
from pathlib import Path

import pandas as pd

from src.domain.entities import Transaction

CATEGORIES = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
TIME_FEATURES = ("hour", "year", "month", "day_of_month", "day_of_week")


def category_key(value):
    if value is None or pd.isna(value) or value == "":
        return "__MISSING__"
    if isinstance(value, (int, float)) and float(value).is_integer():
        return str(int(value))
    return str(value)


def distance_km(transaction: Transaction):
    lat, lon, mlat, mlon = map(math.radians, (transaction.lat, transaction.lon, transaction.merchant_lat, transaction.merchant_lon))
    a = math.sin((mlat-lat)/2)**2 + math.cos(lat)*math.cos(mlat)*math.sin((mlon-lon)/2)**2
    return 6371.009 * 2 * math.asin(math.sqrt(min(1, max(0, a))))


class Preprocessor:
    def __init__(self, path):
        self.artifact = json.loads(Path(path).read_text())
        if self.artifact["version"] != 1:
            raise ValueError("Unsupported preprocessing artifact version")

    def transform(self, transaction: Transaction):
        ts = transaction.transaction_time
        result = dict(zip(TIME_FEATURES, (ts.hour, ts.year, ts.month, ts.day, ts.weekday())))
        for col in CATEGORIES:
            result[col + "_cat"] = self.artifact["categories"][col].get(category_key(getattr(transaction, col)), "cat_NAN")
        for col in (*[col + "_cat" for col in CATEGORIES], *TIME_FEATURES):
            result[col + "_mean_enc"] = self.artifact["target_means"][col].get(
                category_key(result[col]), self.artifact["global_mean"])
        for col in ("amount", "population_city"):
            value = getattr(transaction, col)
            if value is None:
                value = self.artifact["numeric_means"][col]
            result[col + "_log"] = math.log1p(value)
        result["distance_log"] = math.log1p(distance_km(transaction))
        return pd.DataFrame([result], columns=self.artifact["feature_names"])
