{# dbt_date 0.10.1 has no ClickHouse implementation. Keep its public API. #}
{% macro clickhouse__day_of_week(date, isoweek) -%}
{% if isoweek -%}
toDayOfWeek({{ date }}, 0)
{%- else -%}
toDayOfWeek({{ date }}, 3)
{%- endif %}
{%- endmacro %}
