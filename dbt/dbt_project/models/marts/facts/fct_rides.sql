{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'ride_date',
            'data_type': 'date'
        },
        cluster_by=['city_code', 'service_type', 'ride_status']
    )
}}

with ride_enriched as (

    select * from {{ ref('int__ride_enriched') }}

    {% if is_incremental() %}
    where __source_ts_ms > (
        select coalesce(MAX(__source_ts_ms), 0)
        from {{ this }}
    )
    {% endif %}

),

dim_rider as (
    select rider_id, segment from {{ ref('dim_rider') }}
),

dim_driver as (
    select driver_id, driver_status, verification_status from {{ ref('dim_driver') }}
),

dim_vehicle as (
    select vehicle_id, vehicle_type from {{ ref('dim_vehicle') }}
),

dim_date as (
    select date_day, is_weekend, is_public_holiday_id from {{ ref('dim_date') }}
),

dim_promotion as (
    select promotion_id, promo_code, discount_type from {{ ref('dim_promotion') }}
),

final as (

    select
        r.ride_id,
        r.rider_id,
        r.driver_id,
        r.vehicle_id,
        r.promotion_id,
        r.fare_id,

        r.ride_date,
        r.ride_status,
        r.service_type,
        r.city_code,
        r.hour_of_day,
        r.day_of_week,

        r.requested_at,
        r.accepted_at,
        r.arrived_at,
        r.started_at,
        r.completed_at,
        r.cancelled_at,
        r.cancelled_by_user_id,
        r.cancel_reason_code,

        r.is_completed,
        r.is_cancelled,
        r.is_cancelled_by_rider,
        r.is_cancelled_by_driver,
        r.has_surge,
        r.has_promo,

        r.total_fare,
        r.base_fare,
        r.distance_fare,
        r.time_fare,
        r.surge_multiplier,
        r.surge_amount,
        r.discount_amount,
        r.tax_amount,
        r.platform_fee,
        r.driver_earning,
        r.total_fare_before_discount,
        r.total_promo_discount,

        r.distance_km,
        r.duration_min,
        r.duration_minutes,

        r.currency_code,
        r.fare_rule_code,

        r.estimated_distance_km,
        r.estimated_duration_min,

        r.pickup_latitude,
        r.pickup_longitude,
        r.pickup_address,
        r.dropoff_latitude,
        r.dropoff_longitude,
        r.dropoff_address,
        r.dropoff_actual_latitude,
        r.dropoff_actual_longitude,

        dd.is_weekend,
        dd.is_public_holiday_id,
        dr.segment                               as rider_segment,
        ddr.driver_status,
        ddr.verification_status                  as driver_verification_status,
        dv.vehicle_type,
        dp.promo_code,

        r.__source_ts_ms,
        CURRENT_TIMESTAMP()                      as _dbt_loaded_at

    from ride_enriched r
    left join dim_rider     dr  on r.rider_id     = dr.rider_id
    left join dim_driver    ddr on r.driver_id    = ddr.driver_id
    left join dim_vehicle   dv  on r.vehicle_id   = dv.vehicle_id
    left join dim_date      dd  on r.ride_date     = dd.date_day
    left join dim_promotion dp  on r.promotion_id  = dp.promotion_id

)

select * from final
