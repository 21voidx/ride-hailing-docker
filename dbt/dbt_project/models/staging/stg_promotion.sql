-- Staging: Promotion
-- Source    : Batch → dev_bronze_pg.promotion
-- Strategy  : Upsert. Satu baris per promotion_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'promotion') }}
),

renamed as (
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

        -- derived
        promotion_status = 'ACTIVE' as is_active,
        discount_type = 'PERCENT'   as is_percent_discount,
        timestamp_diff(valid_to, valid_from, day) as promo_duration_days

    from source
    where promotion_id is not null
)

select * from renamed
