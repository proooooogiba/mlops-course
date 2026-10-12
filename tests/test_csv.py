import pytest

from scripts.common import normalize, sha256, transactions
from scripts.verify import expected_result
from tests.make_demo import make_demo, row


def test_preserves_cents_postcode_and_optional_target():
    record = normalize(row(amount="39.4"), "test.csv", 1)
    assert record["amount"] == "39.40"
    assert record["post_code"] == "01234"
    assert record["target"] is None
    assert record["transaction_time"] == "2019-01-01 00:00:00"
    assert record["source_row"] == 1
    assert normalize({**row(), "target": "1"}, "train.csv", 2)["target"] == 1


@pytest.mark.parametrize("field,value", [
    ("amount", "NaN"), ("amount", "Infinity"), ("amount", "0.001"),
    ("amount", "-1"), ("amount", "10000000000"), ("amount", "bad"),
    ("us_state", "California"), ("cat_id", ""), ("lat", "91"), ("lon", "nan"),
    ("population_city", "-1"), ("target", "2"), ("transaction_time", "not-a-date"),
])
def test_invalid_data_is_rejected(field, value):
    with pytest.raises(ValueError):
        normalize({**row(), field: value}, "bad.csv", 1)


def test_max_transaction_not_category_sum_and_deterministic_ties(tmp_path):
    make_demo(tmp_path)
    manifest = {"limit_per_file": 0, "files": [
        {"name": name, "rows": count, "sha256": sha256(tmp_path / name)}
        for name, count in (("train.csv", 5), ("test.csv", 3))
    ]}
    assert expected_result(tmp_path, manifest) == [
        {"us_state": "CA", "cat_id": "travel", "max_amount": "100.00"},
        {"us_state": "NY", "cat_id": "kids_pets", "max_amount": "123.45"},
        {"us_state": "TX", "cat_id": "grocery_pos", "max_amount": "0.20"},
    ]
    (tmp_path / "test.csv").write_text("changed")
    with pytest.raises(ValueError, match="checksum"):
        expected_result(tmp_path, manifest)


def test_limit_and_missing_columns(tmp_path):
    make_demo(tmp_path)
    assert len(list(transactions(tmp_path / "train.csv", limit=2))) == 2
    malformed = tmp_path / "malformed.csv"
    malformed.write_text("amount,us_state\n1,CA\n")
    with pytest.raises(ValueError, match="header"):
        list(transactions(malformed))


def test_broken_csv_row_is_not_silently_skipped(tmp_path):
    make_demo(tmp_path)
    path = tmp_path / "test.csv"
    with path.open("a") as stream:
        stream.write("2019-01-01,merchant\n")
    with pytest.raises(ValueError, match="data row 4"):
        list(transactions(path))
