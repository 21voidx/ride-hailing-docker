{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        on_schema_change='append_new_columns'
    )
}}

with rides as (
    select *
    from {{ ref('stg_cdc__rides') }}
    {% if is_incremental() %}
    where TIMESTAMP_MILLIS(__source_ts_ms) > (
        select TIMESTAMP_SUB(MAX(TIMESTAMP_MILLIS(__source_ts_ms)), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

fares as (
    select *
    from {{ ref('stg_pg__ride_fares') }}
    where fare_type = 'FINAL'
),

locations_pivoted as (
    select
        ride_id,
        MAX(case when location_type = 'PICKUP_REQUESTED'  then latitude  end) as pickup_lat,
        MAX(case when location_type = 'PICKUP_REQUESTED'  then longitude end) as pickup_lng,
        MAX(case when location_type = 'DROPOFF_REQUESTED' then latitude  end) as dropoff_lat,
        MAX(case when location_type = 'DROPOFF_REQUESTED' then longitude end) as dropoff_lng
    from {{ ref('stg_pg__ride_locations') }}
    group by ride_id
),

promos as (
    select *
    from {{ ref('stg_pg__promo_usages') }}
),

final as (
    select
        -- identifiers
        r.ride_id,
        r.rider_id,
        r.driver_id,
        r.vehicle_id,

        -- ride attributes
        r.ride_status,
        r.service_type,
        r.city_code,
        r.cancel_reason_code,
        r.cancel_reason_note,
        r.estimated_distance_km,
        r.estimated_duration_min,

        -- timestamps
        r.requested_at,
        r.accepted_at,
        r.arrived_at,
        r.started_at,
        r.completed_at,
        r.cancelled_at,
        r.cancelled_by_user_id,
        r.created_at,
        r.updated_at,

        -- date dimensions
        r.ride_date,
        CAST(EXTRACT(HOUR FROM r.requested_at) AS INT64)        as hour_of_day,
        CAST(EXTRACT(DAYOFWEEK FROM r.requested_at) AS INT64)   as day_of_week,

        -- fare
        f.fare_id,
        f.fare_rule_code,
        f.total_fare,
        f.base_fare,
        f.distance_fare,
        f.time_fare,
        f.surge_multiplier,
        f.surge_amount,
        f.discount_amount,
        f.tax_amount,
        f.platform_fee,
        f.driver_earning,
        f.distance_km,
        f.duration_min,

        -- locations (pivoted)
        l.pickup_lat,
        l.pickup_lng,
        l.dropoff_lat,
        l.dropoff_lng,

        -- promo
        p.promo_usage_id,
        p.promotion_id,
        p.discount_amount_applied as promo_discount_amount,

        -- boolean flags
        r.ride_status = 'COMPLETED'                             as is_completed,
        r.ride_status = 'CANCELLED'                             as is_cancelled,
        (r.ride_status = 'CANCELLED'
         and r.cancelled_by_user_id = r.rider_id)               as is_cancelled_by_rider,
        (r.ride_status = 'CANCELLED'
         and r.cancelled_by_user_id is not null
         and r.cancelled_by_user_id != r.rider_id)              as is_cancelled_by_driver,
        COALESCE(f.surge_multiplier > 1, false)                 as has_surge,
        p.promo_usage_id is not null                            as has_promo,

        -- derived metric
        CAST(
            TIMESTAMP_DIFF(r.completed_at, r.started_at, MINUTE)
        AS NUMERIC)                                             as duration_minutes,

        -- CDC metadata
        r.__op,
        r.__lsn,
        r.__source_ts_ms

    from rides r
    left join fares f          on r.ride_id = f.ride_id
    left join locations_pivoted l on r.ride_id = l.ride_id
    left join promos p          on r.ride_id = p.ride_id
)

select * from final
