{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'ride_date',
            'data_type': 'date'
        },
        cluster_by=['city_code', 'service_type']
    )
}}

with fct_rides as (

    select * from {{ ref('fct_rides') }}

),

dim_date as (

    select date_day, is_weekend, is_public_holiday_id, day_name, month_name, year, quarter
    from {{ ref('dim_date') }}

),

agg as (

    select
        r.ride_date,
        r.city_code,
        r.service_type,

        d.is_weekend,
        d.is_public_holiday_id,
        d.day_name,
        d.month_name,
        d.year,
        d.quarter,

        COUNT(r.ride_id)                                      as total_rides,
        COUNTIF(r.is_completed)                               as completed_rides,
        COUNTIF(r.is_cancelled)                               as cancelled_rides,
        COUNTIF(r.is_cancelled_by_rider)                      as rider_cancellations,
        COUNTIF(r.is_cancelled_by_driver)                     as driver_cancellations,
        COUNTIF(r.has_surge)                                  as surge_rides,
        COUNTIF(r.has_promo)                                  as promo_rides,

        COUNT(DISTINCT r.rider_id)                            as unique_riders,
        COUNT(DISTINCT r.driver_id)                           as unique_drivers,

        SUM(case when r.is_completed then r.total_fare else 0 end)    as gross_revenue,
        SUM(case when r.is_completed then r.platform_fee else 0 end)  as platform_fee_revenue,
        SUM(case when r.is_completed then r.driver_earning else 0 end) as total_driver_earnings,
        SUM(case when r.is_completed then r.discount_amount else 0 end) as total_discounts,
        SUM(case when r.is_completed then r.surge_amount else 0 end)  as total_surge_revenue,

        AVG(case when r.is_completed then r.total_fare end)           as avg_fare,
        AVG(case when r.is_completed then r.distance_km end)          as avg_distance_km,
        AVG(case when r.is_completed then r.duration_minutes end)     as avg_duration_min,
        AVG(case when r.is_completed then r.surge_multiplier end)     as avg_surge_multiplier,

        SAFE_DIVIDE(COUNTIF(r.is_completed), COUNT(r.ride_id))        as completion_rate,
        SAFE_DIVIDE(COUNTIF(r.is_cancelled), COUNT(r.ride_id))        as cancellation_rate

    from fct_rides r
    left join dim_date d on r.ride_date = d.date_day

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

)

select * from agg
