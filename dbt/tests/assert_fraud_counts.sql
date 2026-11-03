{% set fraud_marts = [
    'mart_fraud_by_category', 'mart_fraud_by_state',
    'mart_customer_risk_profile', 'mart_hourly_fraud_pattern',
    'mart_merchant_analytics'
] %}
{% for model in fraud_marts %}
    select '{{ model }}' as model_name
    from {{ ref(model) }}
    where
        fraud_count > labeled_transaction_count
        or labeled_transaction_count > transaction_count
        or (labeled_transaction_count = 0 and fraud_rate is not null)
        or (labeled_transaction_count > 0 and fraud_rate is null)
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
