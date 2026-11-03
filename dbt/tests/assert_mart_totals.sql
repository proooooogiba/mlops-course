select 'mart_daily_state_metrics' as model_name
from {{ ref('mart_daily_state_metrics') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
union all
select 'mart_fraud_by_category' as model_name
from {{ ref('mart_fraud_by_category') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
union all
select 'mart_fraud_by_state' as model_name
from {{ ref('mart_fraud_by_state') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
union all
select 'mart_customer_risk_profile' as model_name
from {{ ref('mart_customer_risk_profile') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
union all
select 'mart_hourly_fraud_pattern' as model_name
from {{ ref('mart_hourly_fraud_pattern') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
union all
select 'mart_merchant_analytics' as model_name
from {{ ref('mart_merchant_analytics') }}
having
    sum(transaction_count) != (select count() from {{ ref('stg_transactions') }})
    or sum(total_amount) != (select sum(staging.amount) from {{ ref('stg_transactions') }} as staging)
