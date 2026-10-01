"""Small in-memory data for unit tests and isolated CI runs."""
import csv
import io


def transaction_csv() -> bytes:
    rows = []
    for index, (amount, hour, merchant_lat, merchant_lon) in enumerate([
        (12, 12, 40.72, -74.01), (80, 14, 40.74, -74.02), (1500, 23, 34.05, -118.24),
        (9000, 2, 51.51, -0.13), (25000, 3, -33.86, 151.21),
    ]):
        rows.append({"transaction_id": f"unit-{index + 1}", "transaction_time": f"2023-01-01 {hour:02}:30:00",
                     "amount": amount, "lat": 40.71, "lon": -74.0, "merchant_lat": merchant_lat,
                     "merchant_lon": merchant_lon, "gender": "F" if index % 2 else "M", "merch": "unit_shop",
                     "cat_id": "shopping_net", "one_city": "New York", "us_state": "NY", "jobs": "Engineer",
                     "population_city": 50000})
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=rows[0])
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode()
