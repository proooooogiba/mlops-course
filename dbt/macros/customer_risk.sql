{% macro customer_risk(rate, labels) -%}
case
    when {{ labels }} = 0 then 'UNKNOWN'
    when {{ rate }} >= (select min_fraud_rate from {{ ref('risk_thresholds') }} where risk_level = 'HIGH') then 'HIGH'
    when {{ rate }} >= (select min_fraud_rate from {{ ref('risk_thresholds') }} where risk_level = 'MEDIUM') then 'MEDIUM'
    else 'LOW'
end
{%- endmacro %}
