with source as (
    select
        promotion_id,
promo_code,
promo_description,
discount_type,
discount_pct,
discount_amount,
max_discount_amount,
min_fare_amount,
usage_limit_total,
usage_limit_per_user,
valid_from,
valid_to,
promotion_status,
created_at,
updated_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'promotion') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by promotion_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
