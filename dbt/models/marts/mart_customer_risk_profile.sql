select
    customer_id,
    count() as transaction_count,
    countIf(is_fraud is not null) as labeled_transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / nullIf(countIf(is_fraud is not null), 0) as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    avg(amount) as average_amount,
    min(transaction_time) as first_transaction_at,
    max(transaction_time) as last_transaction_at,
    {{ customer_risk('fraud_rate', 'labeled_transaction_count') }} as risk_level
from {{ ref('stg_transactions') }}
group by customer_id
