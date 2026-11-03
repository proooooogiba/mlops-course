select
    'mart_fraud_by_category' as model_name,
    fraud_rate
from {{ ref('mart_fraud_by_category') }}
where fraud_rate < 0 or fraud_rate > 100
union all
select
    'mart_fraud_by_state' as model_name,
    fraud_rate
from {{ ref('mart_fraud_by_state') }}
where fraud_rate < 0 or fraud_rate > 100
union all
select
    'mart_customer_risk_profile' as model_name,
    fraud_rate
from {{ ref('mart_customer_risk_profile') }}
where fraud_rate < 0 or fraud_rate > 100
union all
select
    'mart_hourly_fraud_pattern' as model_name,
    fraud_rate
from {{ ref('mart_hourly_fraud_pattern') }}
where fraud_rate < 0 or fraud_rate > 100
union all
select
    'mart_merchant_analytics' as model_name,
    fraud_rate
from {{ ref('mart_merchant_analytics') }}
where fraud_rate < 0 or fraud_rate > 100
