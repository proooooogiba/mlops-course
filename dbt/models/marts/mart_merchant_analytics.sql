select
    merchant,
    count() as transaction_count,
    countIf(is_fraud is not null) as labeled_transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / nullIf(countIf(is_fraud is not null), 0) as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    (labeled_transaction_count >= 20 and fraud_rate >= 5) as is_suspicious
from {{ ref('stg_transactions') }}
group by merchant
