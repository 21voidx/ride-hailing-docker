{{ config(materialized='view') }}

select
    pu.promo_usage_id,
    {{ surrogate_key(["'promo_usage'", 'pu.promo_usage_id']) }} as promo_usage_key,
    pu.ride_id,
    {{ surrogate_key(["'ride'", 'pu.ride_id']) }} as ride_key,
    pu.rider_id,
    {{ surrogate_key(["'rider'", 'pu.rider_id']) }} as rider_key,
    pu.promotion_id,
    {{ surrogate_key(["'promotion'", 'pu.promotion_id']) }} as promotion_key,
    p.promo_code,
    p.discount_type,
    p.promotion_status,
    pu.discount_amount_applied,
    pu.used_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__promo_usage') }} pu
left join {{ ref('stg_pg__promotions') }} p
  on pu.promotion_id = p.promotion_id
qualify row_number() over (partition by pu.ride_id order by pu.used_at desc, pu.promo_usage_id desc) = 1
