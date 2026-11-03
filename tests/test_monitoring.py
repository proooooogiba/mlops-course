"""Monitoring counts committed outcomes, and keeps each test registry isolated."""
from datetime import datetime, timezone

from app.monitoring import ScoringMetrics, WriterMetrics


def test_scoring_metrics_measure_flag_fraction_and_histograms():
    m = ScoringMetrics()
    record = {'transaction_time': datetime.now(timezone.utc).isoformat()}
    m.processed(record, {'score': .1, 'fraud_flag': 0}, .01)
    m.processed(record, {'score': .99, 'fraud_flag': 1}, .02)
    assert m.registry.get_sample_value('fraud_transactions_total') == 2
    assert m.registry.get_sample_value('fraud_flag_ratio') == .5
    assert m.registry.get_sample_value('fraud_score_count') == 2
    assert m.registry.get_sample_value('fraud_event_age_seconds_count') == 2
    assert m.registry.get_sample_value('fraud_processing_seconds_sum') == .03
    assert ScoringMetrics().registry.get_sample_value('fraud_transactions_total') == 0


def test_replayed_score_does_not_inflate_committed_metrics():
    m = WriterMetrics()
    result = {'score': .95, 'fraud_flag': 1}
    m.committed(result, .02, inserted=True)
    m.committed(result, .01, inserted=False)
    assert m.registry.get_sample_value('fraud_db_scores_total') == 1
    assert m.registry.get_sample_value('fraud_db_flagged_total') == 1
    assert m.registry.get_sample_value('fraud_db_score_count') == 1
    assert m.registry.get_sample_value('fraud_db_duplicates_total') == 1
    assert m.registry.get_sample_value('fraud_db_write_seconds_count') == 2
