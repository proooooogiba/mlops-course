select
    us_state,
    count() as transaction_count,
    countIf(is_fraud is not null) as labeled_transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / nullIf(countIf(is_fraud is not null), 0) as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    uniqExact(customer_id) as unique_customers,
    uniqExact(merchant) as unique_merchants
from {{ ref('stg_transactions') }}
group by us_state
