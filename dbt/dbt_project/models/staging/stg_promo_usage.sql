-- Staging: Promo Usage
-- Source    : Batch → dev_bronze_pg.promo_usage
-- Strategy  : Append-only. Satu baris per pemakaian promo per ride.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'promo_usage') }}
),

renamed as (
    select
        promo_usage_id,
        promotion_id,
        ride_id,
        rider_id,
        discount_amount_applied,
        used_at,
        created_at

    from source
    where promo_usage_id is not null
)

select * from renamed
