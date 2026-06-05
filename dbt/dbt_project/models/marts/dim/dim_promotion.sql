{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['promotion_id', 'valid_from']) }} as promotion_sk,
    promotion_snapshot_id,
    promotion_id,
    promo_code,
    promo_description,
    discount_type,
    discount_pct,
    discount_amount,
    max_discount_amount,
    min_fare_amount,
    promo_valid_from,
    promo_valid_to,
    promotion_status,
    date(created_at) as promotion_created_date,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ ref('stg_promotion') }}
