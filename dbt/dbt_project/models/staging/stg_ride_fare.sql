-- Staging: Ride Fare
-- Source    : Batch → dev_bronze_pg.ride_fare
-- Strategy  : Append-only (tiap versi kalkulasi fare disimpan).
--             Gunakan fare_type = 'FINAL' di downstream untuk fare aktual.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'ride_fare') }}
),

renamed as (
    select
        fare_id,
        ride_id,
        fare_type,
        fare_version,
        currency_code,
        distance_km,
        duration_min,
        base_fare,
        distance_fare,
        time_fare,
        surge_multiplier,
        surge_amount,
        discount_amount,
        tax_amount,
        platform_fee,
        driver_earning,
        total_fare,
        fare_rule_code,
        calculated_at,
        created_at,

        -- derived
        surge_multiplier > 1 as is_surge,
        discount_amount  > 0 as has_discount

    from source
    where fare_id is not null
)

select * from renamed
