"""Generate a small labeled stream source with several business segments."""
import argparse
import csv
from pathlib import Path

from tests.training_fixtures import demo_records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('data/train.csv'))
    args = parser.parse_args()
    records = demo_records()
    for index, record in enumerate(records):
        record['target'] = int(index % 20 == 0)
        record['amount'] = 950 if record['target'] else 30
        record['merchant_lat'] = 41 if record['target'] else 40.001
        segment = index % 3
        record.update(us_state=('NY', 'CA', 'TX')[segment],
                      merch=('shop_east', 'shop_west', 'shop_south')[segment],
                      cat_id=('travel', 'grocery', 'shopping')[segment])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    print(f'Wrote {len(records)} labeled transactions to {args.output}')


if __name__ == '__main__':
    main()
