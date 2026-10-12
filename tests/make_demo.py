"""Small synthetic dataset for CI, including equal maxima across files."""

import csv
from pathlib import Path

from scripts.common import CSV_FIELDS


def row(state="CA", category="food_dining", amount="10.10"):
    return dict(zip(CSV_FIELDS, (
        "2019-01-01 00:00", "demo_merchant", category, amount, "Test", "User",
        "F", "Example Street", "Example City", state, "01234", "40", "-73",
        "1000", "Engineer", "40.1", "-73.1",
    )))


def make_demo(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    sources = {
        "train.csv": [
            row("CA", "food_dining", "70.00"),
            row("CA", "food_dining", "70.00"),
            row("CA", "travel", "100.00"),  # max transaction != max category sum
            row("NY", "home", "123.45"),
            row("NY", "kids_pets", "123.45"),  # same-file tie: greater row number wins
        ],
        "test.csv": [
            row("CA", "shopping_net", "100.00"),  # train.csv > test.csv lexicographically
            row("TX", "gas_transport", "0.10"),
            row("TX", "grocery_pos", "0.20"),  # exact cents
        ],
    }
    for name, records in sources.items():
        fields = list(CSV_FIELDS) + (["target"] if name == "train.csv" else [])
        with (directory / name).open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for record in records:
                if name == "train.csv":
                    record["target"] = "0"
                writer.writerow(record)


if __name__ == "__main__":
    make_demo(Path("data"))
