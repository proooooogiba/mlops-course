"""Trigger and recover both provisioned Grafana alerts on the live stack."""
import json
import os
import subprocess
from pathlib import Path

from scripts.verify_hw5 import api, GRAFANA, wait_for


def states():
    response = api(GRAFANA + '/api/prometheus/grafana/api/v1/rules', grafana=True)
    return {rule['name']: rule['state'] for group in response['data']['groups']
            for rule in group.get('rules', []) if rule.get('type') == 'alerting'}


def run(*args, env=None):
    subprocess.run(['docker', 'compose', *args], env=env, check=True)


def main():
    evidence = {}
    high = 'Fraud share above 15%'
    stopped = 'Scoring stream stopped'
    try:
        # Scorer keeps its Kafka offsets; only the flag threshold changes.
        run('up', '-d', '--no-deps', 'scorer', env={**os.environ, 'FRAUD_THRESHOLD': '0'})
        wait_for(lambda: states().get(high) == 'firing', timeout=180)
        evidence['high_ratio_firing'] = states()
        run('stop', 'simulator')
        wait_for(lambda: states().get(stopped) == 'firing', timeout=180)
        evidence['stopped_stream_firing'] = states()
    finally:
        restored = {**os.environ}
        restored.pop('FRAUD_THRESHOLD', None)
        run('up', '-d', '--no-deps', 'scorer', 'simulator', env=restored)
    wait_for(lambda: states().get(high) == 'inactive' and states().get(stopped) == 'inactive', timeout=180)
    evidence['recovered'] = states()
    path = Path('runtime/hw5/alerts-verification.json')
    path.write_text(json.dumps(evidence, indent=2))
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
