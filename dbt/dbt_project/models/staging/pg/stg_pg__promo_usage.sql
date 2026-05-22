with source as (
    select
        promo_usage_id,
promotion_id,
ride_id,
rider_id,
discount_amount_applied,
used_at,
created_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'promo_usage') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by promo_usage_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
