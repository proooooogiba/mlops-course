with normalized as (
    select
        toDateTime(transaction_time, 'UTC') as transaction_time,
        trimBoth(toString(merch)) as merchant,
        lowerUTF8(trimBoth(toString(cat_id))) as category,
        toDecimal64(toString(amount), 2) as amount,
        trimBoth(toString(name_1)) as first_name,
        trimBoth(toString(name_2)) as last_name,
        upperUTF8(trimBoth(toString(gender))) as gender,
        upperUTF8(trimBoth(toString(us_state))) as us_state,
        toFloat64(lat) as latitude,
        toFloat64(lon) as longitude,
        toFloat64(merchant_lat) as merchant_latitude,
        toFloat64(merchant_lon) as merchant_longitude,
        cast(target, 'Nullable(UInt8)') as is_fraud
    from {{ source('raw', 'transactions') }}
)

select
    *,
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'latitude', 'longitude']) }} as customer_id,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    {{ dbt_date.day_of_week('transaction_time', isoweek=true) }} as day_of_week,
    {{ amount_bucket('amount') }} as amount_bucket
from normalized
