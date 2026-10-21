"""Synthetic labeled transactions, with no redistributed Kaggle data."""

def demo_records():
    rows = []
    for month in ("2020-01", "2020-02", "2020-03"):
        for index in range(100):
            label = int(index % 5 == 0)
            rows.append({"transaction_time": f"{month}-{min(index % 31 + 1, 28):02d}T12:30:00",
                         "amount": 900 + index if label else 10 + index,
                         "population_city": 1000, "lat": 40, "lon": -70,
                         "merchant_lat": 41 if label else 40.001, "merchant_lon": -70,
                         "cat_id": "travel", "us_state": "NY", "gender": "F",
                         "merch": "shop", "jobs": "engineer", "target": label})
    return rows
