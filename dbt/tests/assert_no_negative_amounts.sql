select * from {{ ref('stg_transactions') }}
where amount < 0
