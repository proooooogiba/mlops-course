select
    transaction_date,
    us_state,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    arrayReduce('quantileExact(0.95)', groupArray(amount)) as p95_amount,
    100.0 * countIf(amount_bucket = 'LARGE') / count() as large_transaction_share
from {{ ref('stg_transactions') }}
group by transaction_date, us_state
