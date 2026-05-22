{{ config(materialized='view') }}

select
    pu.promo_usage_id,
    pu.ride_id,
    pu.rider_id,
    pu.promotion_id,
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
