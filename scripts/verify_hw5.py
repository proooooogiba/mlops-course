"""Check a running HW5 stack: live growth, all panel queries and hot reload."""
import argparse
import base64
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROMETHEUS = os.getenv('PROMETHEUS_URL', 'http://localhost:9090')
GRAFANA = os.getenv('GRAFANA_URL', 'http://localhost:3000')


def api(url, data=None, grafana=False):
    headers = {'Content-Type': 'application/json'}
    if grafana:
        headers['Authorization'] = 'Basic ' + base64.b64encode(b'admin:admin').decode()
    request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def query(expression):
    result = api(PROMETHEUS + '/api/v1/query?' + urllib.parse.urlencode({'query': expression}))
    assert result['status'] == 'success', result
    return result['data']['result']


def pg(query_sql):
    payload = {'from': str(int((time.time() - 900) * 1000)), 'to': str(int(time.time() * 1000)),
               'queries': [{'refId': 'A', 'datasource': {'uid': 'scores-postgres', 'type': 'postgres'},
                            'rawSql': query_sql, 'format': 'table', 'rawQuery': True, 'intervalMs': 1000,
                            'maxDataPoints': 1000}]}
    response = api(GRAFANA + '/api/ds/query', payload, grafana=True)
    result = response['results']['A']
    assert not result.get('error'), result
    return result.get('frames', [])


def frame_rows(frames):
    return [row for f in frames for row in zip(*f.get('data', {}).get('values', []))]


def compose(*args):
    return subprocess.check_output(['docker', 'compose', *args], text=True).strip()


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def wait_for(check, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(2)
    raise TimeoutError('Condition not met within timeout')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reload', action='store_true', help='Actually retrain and verify hot reload')
    args = parser.parse_args()
    targets = api(PROMETHEUS + '/api/v1/targets')['data']['activeTargets']
    assert {t['labels']['job'] for t in targets if t['health'] == 'up'} == {'scorer', 'score-writer', 'node-exporter'}, targets
    before = float(query('fraud_transactions_total')[0]['value'][1])
    db_before = frame_rows(pg('SELECT count(*) AS count FROM scores'))[0][0]
    wait_for(lambda: float(query('fraud_transactions_total')[0]['value'][1]) > before)
    wait_for(lambda: frame_rows(pg('SELECT count(*) AS count FROM scores'))[0][0] > db_before)
    # Choose actual rows for nonempty filters, and exercise SQL escaping as Grafana does.
    states = [r[0] for r in frame_rows(pg('SELECT DISTINCT us_state FROM scores ORDER BY 1'))]
    merchants = [r[0] for r in frame_rows(pg('SELECT DISTINCT merch FROM scores ORDER BY 1'))]
    state, merchant = frame_rows(pg('SELECT us_state, merch FROM scores ORDER BY written_at DESC LIMIT 1'))[0]
    checked_panels = 0
    for path in sorted(Path('monitoring/grafana/dashboards').glob('*.json')):
        dashboard = json.loads(path.read_text())
        remote = api(GRAFANA + '/api/dashboards/uid/' + dashboard['uid'], grafana=True)
        assert remote['meta']['provisioned'], remote['meta']
        for panel in dashboard['panels']:
            for target in panel['targets']:
                if target['datasource']['uid'] == 'prometheus':
                    assert query(target['expr']), (dashboard['uid'], panel['title'], target['expr'])
                else:
                    raw = target['rawSql'].replace('${state:sqlstring}', ','.join(map(literal, states))).replace('${merchant:sqlstring}', ','.join(map(literal, merchants)))
                    assert frame_rows(pg(raw)), (dashboard['uid'], panel['title'])
                    if '${state:sqlstring}' in target['rawSql']:
                        selected = target['rawSql'].replace('${state:sqlstring}', literal(state)).replace('${merchant:sqlstring}', literal(merchant))
                        assert frame_rows(pg(selected)), ('empty selected state/merchant', state, merchant)
            checked_panels += 1
    rules = api(GRAFANA + '/api/v1/provisioning/alert-rules', grafana=True)
    assert {r['uid'] for r in rules} >= {'fraud-high-ratio', 'fraud-stream-stopped'}, rules
    # A Kafka replay must not add a second PostgreSQL row.
    duplicate_result = compose('exec', '-T', 'score-writer', 'python', '-c',
        "import os,psycopg; from psycopg.rows import dict_row; from app.score_writer import write_score; "
        "c=psycopg.connect(os.environ['SCORES_DSN'],autocommit=True,row_factory=dict_row); "
        "r=c.execute('SELECT * FROM scores LIMIT 1').fetchone(); assert r; "
        "assert write_score(c,r) is False; print('duplicate ignored')")
    evidence = {'targets': '3/3 up', 'live_growth': True, 'panels_with_data': checked_panels,
                'filters': {'state': state, 'merchant': merchant}, 'alerts': [r['uid'] for r in rules],
                'postgres_replay': duplicate_result}
    if args.reload:
        old = query('fraud_model_active == 1')[0]['metric']['version']
        container = compose('ps', '-q', 'scorer')
        restarts = subprocess.check_output(['docker', 'inspect', '--format', '{{.RestartCount}}', container], text=True).strip()
        published = json.loads(compose('exec', '-T', 'scorer', 'python', '-m', 'scripts.retrain_hw5'))['published_version']
        wait_for(lambda: any(r['metric']['version'] == published for r in query('fraud_model_active == 1')))
        assert old != published
        assert compose('ps', '-q', 'scorer') == container
        assert subprocess.check_output(['docker', 'inspect', '--format', '{{.RestartCount}}', container], text=True).strip() == restarts
        assert float(query(f'fraud_model_loads_total{{version="{published}"}}')[0]['value'][1]) == 1
        assert query(f'fraud_model_loaded_at_seconds{{version="{published}"}} > (time() - 30)'), 'No model annotation event'
        wait_for(lambda: frame_rows(pg('SELECT count(*) FROM scores WHERE model_version=' + literal(published)))[0][0] > 0)
        evidence['reload'] = {'before': old, 'after': published, 'same_container': container, 'unchanged_restart_count': restarts, 'annotation': True}
    out = Path('runtime/hw5/verification.json')
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
