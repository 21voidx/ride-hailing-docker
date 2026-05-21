with source as (

    select * from {{ source('bronze_pg', 'ride_fare') }}

),

final_fares as (

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

        (base_fare + distance_fare + time_fare + surge_amount) as total_fare_before_discount,

        _ingested_at,
        _source_system

    from source
    where fare_type = 'FINAL'

)

select * from final_fares
