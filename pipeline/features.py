"""Own stateless preprocessing: cyclic time, log money, distance, population."""
import numpy as np
import pandas as pd

CATEGORICAL = ["cat_id", "us_state", "gender", "merch", "jobs"]


def features(records):
    """Read raw transactions; return the same label-free features in train and inference."""
    frame = pd.DataFrame(records)
    out = pd.DataFrame(index=frame.index)
    for name in CATEGORICAL:
        out[name] = frame[name].fillna("unknown").astype(str).str.strip().str.lower()
    amount = pd.to_numeric(frame["amount"], errors="raise").fillna(0).clip(lower=0)
    population = pd.to_numeric(frame["population_city"], errors="raise").fillna(0).clip(lower=0)
    out["log_amount"] = np.log1p(amount)
    out["log_population"] = np.log1p(population)
    out["amount_per_resident"] = amount / (population + 1)
    timestamps = pd.to_datetime(frame["transaction_time"], utc=True, errors="raise")
    hour = timestamps.dt.hour + timestamps.dt.minute / 60
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["weekday_sin"] = np.sin(2 * np.pi * timestamps.dt.dayofweek / 7)
    out["weekday_cos"] = np.cos(2 * np.pi * timestamps.dt.dayofweek / 7)
    out["weekend"] = (timestamps.dt.dayofweek >= 5).astype(int)
    lat, lon, mlat, mlon = [np.radians(pd.to_numeric(frame[n], errors="raise"))
                            for n in ("lat", "lon", "merchant_lat", "merchant_lon")]
    a = np.sin((mlat-lat)/2)**2 + np.cos(lat)*np.cos(mlat)*np.sin((mlon-lon)/2)**2
    out["distance_km"] = 6371 * 2 * np.arcsin(np.sqrt(a.clip(0, 1)))
    if not np.isfinite(out.drop(columns=CATEGORICAL).to_numpy()).all():
        raise ValueError("nonfinite numeric features")
    return out
