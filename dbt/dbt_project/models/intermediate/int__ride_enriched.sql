{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        on_schema_change='sync_all_columns'
    )
}}

with rides as (

    select * from {{ ref('stg_cdc__rides') }}

    {% if is_incremental() %}
    where __source_ts_ms > (
        select coalesce(MAX(__source_ts_ms), 0)
        from {{ this }}
    )
    {% endif %}

),

fares as (

    select * from {{ ref('stg_pg__ride_fares') }}

),

locations as (

    select * from {{ ref('stg_pg__ride_locations') }}

),

promo_usages as (

    select
        ride_id,
        SUM(discount_amount_applied) as total_promo_discount,
        COUNT(*)                     as promo_usage_count,
        MAX(promotion_id)            as promotion_id
    from {{ ref('stg_pg__promo_usages') }}
    group by 1

),

enriched as (

    select
        r.ride_id,
        r.rider_id,
        r.driver_id,
        r.vehicle_id,
        r.ride_status,
        r.service_type,
        r.city_code,

        r.requested_at,
        r.accepted_at,
        r.arrived_at,
        r.started_at,
        r.completed_at,
        r.cancelled_at,

        r.cancelled_by_user_id,
        r.cancel_reason_code,
        r.cancel_reason_note,

        r.estimated_distance_km,
        r.estimated_duration_min,

        r.created_at,
        r.updated_at,
        r.ride_date,

        EXTRACT(HOUR FROM r.requested_at AT TIME ZONE 'Asia/Jakarta') as hour_of_day,
        EXTRACT(DAYOFWEEK FROM r.ride_date)                            as day_of_week,

        (r.ride_status = 'COMPLETED')                                  as is_completed,
        (r.ride_status = 'CANCELLED')                                  as is_cancelled,
        (r.ride_status = 'CANCELLED'
            and r.cancelled_by_user_id = r.rider_id)                   as is_cancelled_by_rider,
        (r.ride_status = 'CANCELLED'
            and r.cancelled_by_user_id = r.driver_id)                  as is_cancelled_by_driver,

        f.fare_id,
        f.currency_code,
        f.distance_km,
        f.duration_min,
        f.base_fare,
        f.distance_fare,
        f.time_fare,
        f.surge_multiplier,
        f.surge_amount,
        f.discount_amount,
        f.tax_amount,
        f.platform_fee,
        f.driver_earning,
        f.total_fare,
        f.total_fare_before_discount,
        f.fare_rule_code,

        (f.surge_multiplier > 1)                                       as has_surge,
        (pu.ride_id is not null)                                       as has_promo,

        pu.promotion_id,
        pu.total_promo_discount,

        SAFE_DIVIDE(
            TIMESTAMP_DIFF(r.completed_at, r.started_at, SECOND),
            60.0
        )                                                              as duration_minutes,

        l.pickup_latitude,
        l.pickup_longitude,
        l.pickup_address,
        l.pickup_place_id,
        l.dropoff_latitude,
        l.dropoff_longitude,
        l.dropoff_address,
        l.dropoff_place_id,
        l.pickup_actual_latitude,
        l.pickup_actual_longitude,
        l.pickup_actual_address,
        l.dropoff_actual_latitude,
        l.dropoff_actual_longitude,
        l.dropoff_actual_address,

        r.__source_ts_ms,
        r.__op,
        r.__lsn

    from rides r
    left join fares     f  on r.ride_id = f.ride_id
    left join locations l  on r.ride_id = l.ride_id
    left join promo_usages pu on r.ride_id = pu.ride_id

)

select * from enriched
