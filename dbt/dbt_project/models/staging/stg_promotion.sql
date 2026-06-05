with src as (select * from {{ ref('snap_promotion') }})
select
    cast(dbt_scd_id as string) as promotion_snapshot_id,
    cast(promotion_id as int64) as promotion_id,
    upper(cast(promo_code as string)) as promo_code,
    cast(promo_description as string) as promo_description,
    upper(cast(discount_type as string)) as discount_type,
    cast(discount_pct as numeric) as discount_pct,
    cast(discount_amount as numeric) as discount_amount,
    cast(max_discount_amount as numeric) as max_discount_amount,
    cast(min_fare_amount as numeric) as min_fare_amount,
    cast(valid_from as timestamp) as promo_valid_from,
    cast(valid_to as timestamp) as promo_valid_to,
    upper(cast(promotion_status as string)) as promotion_status,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    deleted_at is not null as is_deleted
from src
