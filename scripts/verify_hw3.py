"""End-to-end manual Airflow runs and score-version assertion; execute on the host."""
import argparse
import base64
import http.client
import json
import os
import sqlite3
import time
import urllib.parse
import urllib.request
import urllib.error
import uuid
from pathlib import Path


def request(path, body=None):
    url = os.getenv('AIRFLOW_URL', 'http://localhost:8081') + '/api/v1/' + path
    headers = {'Authorization': 'Basic ' + base64.b64encode(b'admin:admin').decode(), 'Content-Type': 'application/json'}
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def run_dag(dag_id):
    run_id = 'manual__verify_' + uuid.uuid4().hex
    request(f'dags/{dag_id}/dagRuns', {'dag_run_id': run_id})
    for _ in range(180):
        result = request(f'dags/{dag_id}/dagRuns/' + urllib.parse.quote(run_id, safe=''))
        if result['state'] == 'success':
            tasks = request(f'dags/{dag_id}/dagRuns/{run_id}/taskInstances')['task_instances']
            assert all(t['state'] == 'success' for t in tasks), tasks
            print(dag_id, run_id, 'success', flush=True)
            return run_id
        if result['state'] == 'failed':
            raise AssertionError(f'{dag_id} failed: {result}')
        time.sleep(2)
    raise TimeoutError(dag_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--require-publish', action='store_true', help='Require candidate acceptance (synthetic CI scenario)')
    args = parser.parse_args()
    shared = Path(os.getenv('SHARED_DIR', 'runtime'))
    for attempt in range(60):
        try:
            dags = request('dags')['dags']
            if {'supply_labeled_batch', 'retrain_fraud', 'dataset_quality'} <= {d['dag_id'] for d in dags}:
                break
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            pass
        time.sleep(2)
    else:
        raise TimeoutError('Airflow did not register all three DAGs')
    before = json.loads((shared / 'models/latest.json').read_text())
    run_dag('supply_labeled_batch')
    training_run = run_dag('retrain_fraud')
    run_dag('dataset_quality')
    after = json.loads((shared / 'models/latest.json').read_text())
    assert (shared / 'models' / after['file']).is_file()
    assert 'PASS' in (shared / 'reports/latest.html').read_text()
    with sqlite3.connect(f"file:{shared / 'warehouse/fraud.db'}?mode=ro", uri=True) as conn:
        row = conn.execute('SELECT status,metrics FROM training_runs WHERE run_key=?', (training_run,)).fetchone()
        assert row is not None
        decision = json.loads(row[1])
        if args.require_publish:
            assert row[0] == 'published', decision
        if row[0] == 'rejected':
            assert not decision['accepted']
            print('Candidate correctly rejected by quality gate; champion stays', after['version'], flush=True)
        else:
            assert row[0] == 'published' and decision['accepted']
            assert after['version'] != before['version']
    # Wait for new Kafka transactions scored by the current model, without container restart.
    for _ in range(60):
        with sqlite3.connect(f"file:{shared / 'warehouse/fraud.db'}?mode=ro", uri=True) as conn:
            count = conn.execute('SELECT count(*) FROM scores WHERE model_version=?', (after['version'],)).fetchone()[0]
        if count:
            break
        time.sleep(2)
    assert count > 0, 'No versioned scores from the running Kafka scorer'
    evidence = {'before': before, 'after': after, 'training_run': training_run,
                'decision': decision, 'versioned_scores': count, 'dags': 'all manual runs successful'}
    (shared / 'verification.json').write_text(json.dumps(evidence, indent=2))
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
