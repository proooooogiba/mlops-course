"""Write a three-month labeled synthetic dataset only into an empty target path."""
import argparse
from pathlib import Path
import pandas as pd
from tests.training_fixtures import demo_records

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/train.csv"))
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"refusing to overwrite {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(demo_records()).to_csv(args.output, index=False)
