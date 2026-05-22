with source as (
    select * from {{ ref('stg_pg__promotions') }}
),

final as (
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
        is_currently_valid,
        created_at,
        updated_at
    from source
)

select * from final
