select
    cast(promo_usage_id as int64) as promo_usage_id,
    cast(promotion_id as int64) as promotion_id,
    cast(ride_id as int64) as ride_id,
    cast(rider_id as int64) as rider_id,
    cast(discount_amount_applied as numeric) as discount_amount_applied,
    cast(used_at as timestamp) as used_at,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('bronze_mysql', 'promo_usage') }}
