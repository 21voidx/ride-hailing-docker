-- Expose ALL fare rows — ESTIMATED, FINAL, ADJUSTED.
-- Business filter (fare_type = 'FINAL') happens in int__ride_enriched.
with source as (
    select * from {{ source('dev_bronze_pg', 'ride_fare') }}
),

final as (
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
        created_at
    from source
)

select * from final
