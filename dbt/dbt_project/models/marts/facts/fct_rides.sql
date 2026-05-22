{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        on_schema_change='append_new_columns'
    )
}}

with rides as (
    select *
    from {{ ref('int__ride_enriched') }}
    {% if is_incremental() %}
    where TIMESTAMP_MILLIS(__source_ts_ms) > (
        select TIMESTAMP_SUB(MAX(TIMESTAMP_MILLIS(__source_ts_ms)), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

dim_rider as (
    select user_id, segment, cohort_month
    from {{ ref('dim_rider') }}
),

dim_driver as (
    select driver_id, active_vehicle_type, all_docs_verified
    from {{ ref('dim_driver') }}
),

dim_vehicle as (
    select vehicle_id, vehicle_type, vehicle_make, vehicle_model
    from {{ ref('dim_vehicle') }}
),

dim_date as (
    select date_day, is_weekend, is_public_holiday, season
    from {{ ref('dim_date') }}
),

dim_promotion as (
    select promotion_id, promo_code, discount_type
    from {{ ref('dim_promotion') }}
),

final as (
    select
        r.ride_id,
        r.rider_id,
        r.driver_id,
        r.vehicle_id,
        r.promotion_id,

        -- ride attributes
        r.ride_status,
        r.service_type,
        r.city_code,
        r.cancel_reason_code,

        -- date/time
        r.ride_date,
        r.hour_of_day,
        r.day_of_week,
        r.requested_at,
        r.accepted_at,
        r.arrived_at,
        r.started_at,
        r.completed_at,
        r.cancelled_at,

        -- dim_date enrichment
        dd.is_weekend,
        dd.is_public_holiday,
        dd.season,

        -- rider dim
        dr.segment                                                      as rider_segment,
        dr.cohort_month                                                 as rider_cohort_month,

        -- fare measures
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
        r.distance_km,
        r.duration_min,
        r.duration_minutes,

        -- promo
        r.promo_discount_amount,
        dp.promo_code,
        dp.discount_type                                                as promo_discount_type,

        -- boolean flags
        r.is_completed,
        r.is_cancelled,
        r.is_cancelled_by_rider,
        r.is_cancelled_by_driver,
        r.has_surge,
        r.has_promo,

        -- CDC metadata
        r.__lsn,
        r.__source_ts_ms

    from rides r
    left join dim_rider dr         on r.rider_id       = dr.user_id
    left join dim_driver ddr       on r.driver_id      = ddr.driver_id
    left join dim_vehicle dv       on r.vehicle_id     = dv.vehicle_id
    left join dim_date dd          on r.ride_date      = dd.date_day
    left join dim_promotion dp     on r.promotion_id   = dp.promotion_id
)

select * from final
