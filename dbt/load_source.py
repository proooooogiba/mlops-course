"""Load labeled HW2 CSV into the isolated HW4 source; refuse to duplicate rows."""
import argparse
import csv
import io
import os
import urllib.request
import base64


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv_file')
    args = parser.parse_args()
    host = os.getenv('DBT_CH_HOST', 'localhost')
    port = os.getenv('DBT_CH_PORT', '8123')
    user = os.getenv('DBT_CH_USER', 'homework')
    password = os.getenv('DBT_CH_PASSWORD', 'homework')
    auth = base64.b64encode(f'{user}:{password}'.encode()).decode()

    def query(sql, data=b''):
        from urllib.parse import urlencode
        url = f'http://{host}:{port}/?' + urlencode({'query': sql})
        req = urllib.request.Request(url, data=data, headers={'Authorization': f'Basic {auth}'})
        return urllib.request.urlopen(req, timeout=120).read().decode().strip()

    query('CREATE DATABASE IF NOT EXISTS transactions_db')
    query('''CREATE TABLE IF NOT EXISTS transactions_db.transactions (
        transaction_time DateTime('UTC'), merch String, cat_id String,
        amount Decimal(18,2), name_1 String, name_2 String, gender String,
        us_state String, lat Float64, lon Float64, merchant_lat Float64,
        merchant_lon Float64, target Nullable(UInt8)
    ) ENGINE=MergeTree ORDER BY (us_state, transaction_time)''')
    if int(query('SELECT count() FROM transactions_db.transactions')):
        raise SystemExit('Source is not empty: refusing duplicate ingestion. Use existing source or a fresh HW4 volume.')
    columns = ['transaction_time', 'merch', 'cat_id', 'amount', 'name_1',
               'name_2', 'gender', 'us_state', 'lat', 'lon', 'merchant_lat',
               'merchant_lon', 'target']
    inserted = 0
    with open(args.csv_file, newline='', encoding='utf-8-sig') as source:
        reader = csv.DictReader(source)
        missing = set(columns) - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f'Missing CSV columns: {sorted(missing)}')
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in reader:
            values = [row[c] for c in columns]
            if len(values[0]) == 16:
                values[0] += ':00'
            if values[-1] == '':
                values[-1] = r'\N'
            writer.writerow(values)
            inserted += 1
            if inserted % 10000 == 0:
                query('INSERT INTO transactions_db.transactions FORMAT CSV', buf.getvalue().encode())
                buf.seek(0)
                buf.truncate()
        if buf.tell():
            query('INSERT INTO transactions_db.transactions FORMAT CSV', buf.getvalue().encode())
    print(f'Loaded {inserted} transactions from {args.csv_file}')


if __name__ == '__main__':
    main()
