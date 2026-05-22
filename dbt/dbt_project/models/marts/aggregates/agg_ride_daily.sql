{{
    config(
        materialized='table',
        partition_by={'field': 'ride_date', 'data_type': 'date'}
    )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

agg as (
    select
        r.city_code,
        r.service_type,
        r.ride_date,
        COUNT(*)                                    as total_rides,
        COUNTIF(r.is_completed)                     as completed_rides,
        COUNTIF(r.is_cancelled)                     as cancelled_rides,
        COUNTIF(r.is_cancelled_by_rider)            as cancelled_by_rider,
        COUNTIF(r.is_cancelled_by_driver)           as cancelled_by_driver,
        COUNTIF(r.has_surge)                        as surge_rides,
        COUNTIF(r.has_promo)                        as promo_rides,
        COALESCE(SUM(r.total_fare), 0)              as total_revenue,
        COALESCE(AVG(r.total_fare), 0)              as avg_fare,
        COALESCE(SUM(r.distance_km), 0)             as total_distance_km,
        COALESCE(AVG(r.duration_minutes), 0)        as avg_duration_min,
        COALESCE(AVG(r.surge_multiplier), 1)        as avg_surge_multiplier,
        d.is_weekend,
        d.is_public_holiday,
        d.season
    from rides r
    left join dim_date d on r.ride_date = d.date_day
    group by
        r.city_code,
        r.service_type,
        r.ride_date,
        d.is_weekend,
        d.is_public_holiday,
        d.season
)

select
    {{ dbt_utils.generate_surrogate_key(['city_code', 'service_type', 'ride_date']) }}
                                                    as agg_key,
    *
from agg
