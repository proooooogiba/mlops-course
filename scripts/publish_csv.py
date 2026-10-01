import argparse
from pathlib import Path

from app import bootstrap, settings
from src.adapters.csv_input import read_transactions


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path, nargs="?", default=settings.TEST_CSV_PATH)
    parser.add_argument("--limit", type=int, default=100, help="First N rows; 0 means the entire CSV")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    transactions = read_transactions(args.csv.read_bytes(), limit=args.limit or None)
    with bootstrap.build_submit_transactions() as use_case:
        count = use_case.execute(transactions)
    print(f"Published {count} transactions to {settings.TRANSACTIONS_TOPIC}")
