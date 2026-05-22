with source as (
    select * from {{ source('dev_bronze_pg', 'promotion') }}
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
        created_at,
        updated_at,
        (
            promotion_status = 'ACTIVE'
            and valid_from <= CURRENT_TIMESTAMP()
            and valid_to >= CURRENT_TIMESTAMP()
        )                                        as is_currently_valid
    from source
)

select * from final
