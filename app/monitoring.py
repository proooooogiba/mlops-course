"""Prometheus metrics; business dimensions stay in PostgreSQL, not label sets."""
import time
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

SCORE_BUCKETS = tuple(i / 20 for i in range(21))
LATENCY_BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30)


class ScoringMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.transactions = Counter('fraud_transactions_total', 'Successfully scored and published transactions', registry=self.registry)
        self.duration = Histogram('fraud_processing_seconds', 'Processing through Kafka delivery and offset commit', buckets=LATENCY_BUCKETS, registry=self.registry)
        self.scores = Histogram('fraud_score', 'Predicted fraud probability', buckets=SCORE_BUCKETS, registry=self.registry)
        self.fraud = Counter('fraud_flagged_total', 'Transactions predicted as fraud', registry=self.registry)
        self.ratio = Gauge('fraud_flag_ratio', 'Flagged fraction since process start', registry=self.registry)
        self.errors = Counter('fraud_scoring_errors_total', 'Invalid input records sent to dead_letters', registry=self.registry)
        self.age = Histogram('fraud_event_age_seconds', 'Event age at scoring; includes input queue delay', buckets=(.1, .5, 1, 2, 5, 10, 30, 60, 300, 3600), registry=self.registry)
        self.loads = Counter('fraud_model_loads_total', 'Successful model loads by version', ['version'], registry=self.registry)
        self.loaded_at = Gauge('fraud_model_loaded_at_seconds', 'Unix time of last successful load per version', ['version'], registry=self.registry)
        self.active = Gauge('fraud_model_active', 'One for the active model version, zero for previous versions', ['version'], registry=self.registry)
        self.total_count = self.fraud_count = 0

    def model_loaded(self, version, previous):
        if previous is not None:
            self.active.labels(version=previous).set(0)
        self.active.labels(version=version).set(1)
        self.loads.labels(version=version).inc()
        self.loaded_at.labels(version=version).set(time.time())

    def processed(self, record, result, duration):
        self.total_count += 1
        self.fraud_count += result['fraud_flag']
        self.transactions.inc()
        self.duration.observe(duration)
        self.scores.observe(result['score'])
        self.fraud.inc(result['fraud_flag'])
        self.ratio.set(self.fraud_count / self.total_count)
        timestamp = datetime.fromisoformat(record['transaction_time'].replace('Z', '+00:00'))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        self.age.observe(max(0, time.time() - timestamp.timestamp()))


class WriterMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.writes = Counter('fraud_db_scores_total', 'New score rows committed to PostgreSQL', registry=self.registry)
        self.duration = Histogram('fraud_db_write_seconds', 'PostgreSQL insert and commit duration', buckets=LATENCY_BUCKETS, registry=self.registry)
        self.fraud = Counter('fraud_db_flagged_total', 'New committed rows flagged as fraud', registry=self.registry)
        self.scores = Histogram('fraud_db_score', 'Scores of newly committed rows', buckets=SCORE_BUCKETS, registry=self.registry)
        self.duplicates = Counter('fraud_db_duplicates_total', 'Replayed transactions already in PostgreSQL', registry=self.registry)
        self.errors = Counter('fraud_db_errors_total', 'PostgreSQL write failures', registry=self.registry)

    def committed(self, result, duration, inserted):
        self.duration.observe(duration)
        if inserted:
            self.writes.inc()
            self.fraud.inc(result['fraud_flag'])
            self.scores.observe(result['score'])
        else:
            self.duplicates.inc()


def serve(metrics, port):
    start_http_server(port, addr='0.0.0.0', registry=metrics.registry)
