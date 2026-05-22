with source as (
    select * from {{ source('dev_bronze_pg', 'promo_usage') }}
),

final as (
    select
        promo_usage_id,
        promotion_id,
        ride_id,
        rider_id,
        discount_amount_applied,
        used_at,
        created_at
    from source
)

select * from final
