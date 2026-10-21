"""Print the published model, trained pointer, month counts and recent scores."""
import json
from pipeline.storage import db, root

if __name__ == '__main__':
    with db() as conn:
        result = {
            "published": json.loads((root() / 'models/latest.json').read_text()),
            "state": dict(conn.execute('SELECT key,value FROM state').fetchall()),
            "months": [dict(r) for r in conn.execute('SELECT substr(transaction_time,1,7) AS month,count(*) AS rows FROM labeled_transactions GROUP BY 1 ORDER BY 1')],
            "scores": [dict(r) for r in conn.execute('SELECT * FROM scores ORDER BY received_at DESC LIMIT 5')],
            "runs": [dict(r) for r in conn.execute('SELECT run_key,month,status,version FROM training_runs ORDER BY rowid DESC LIMIT 5')],
        }
    print(json.dumps(result, indent=2))
