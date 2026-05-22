{{ config(cluster_by=['promo_code', 'promotion_status']) }}

select
    promotion_id,
    {{ surrogate_key(["'promotion'", 'promotion_id']) }} as promotion_key,
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
    {{ audit_columns() }}
from {{ ref('stg_pg__promotions') }}
