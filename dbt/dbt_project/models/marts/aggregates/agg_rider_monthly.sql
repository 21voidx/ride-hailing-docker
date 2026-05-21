{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'activity_month',
            'data_type': 'date'
        },
        cluster_by=['rider_id']
    )
}}

with fct_rides as (

    select * from {{ ref('fct_rides') }}

),

dim_rider as (

    select
        rider_id,
        segment,
        cohort_month,
        signup_date,
        account_status

    from {{ ref('dim_rider') }}

),

monthly_agg as (

    select
        r.rider_id,
        DATE_TRUNC(r.ride_date, MONTH)                          as activity_month,

        COUNT(r.ride_id)                                        as total_rides,
        COUNTIF(r.is_completed)                                 as completed_rides,
        COUNTIF(r.is_cancelled)                                 as cancelled_rides,
        COUNTIF(r.has_promo)                                    as promo_rides,
        COUNTIF(r.has_surge)                                    as surge_rides,

        COUNT(DISTINCT r.city_code)                             as cities_ridden,
        COUNT(DISTINCT r.service_type)                          as service_types_used,

        SUM(case when r.is_completed then r.total_fare else 0 end)   as total_spend,
        AVG(case when r.is_completed then r.total_fare end)          as avg_fare,
        SUM(case when r.is_completed then r.distance_km else 0 end)  as total_distance_km,
        AVG(case when r.is_completed then r.duration_minutes end)    as avg_trip_duration_min

    from fct_rides r
    group by 1, 2

),

final as (

    select
        ma.rider_id,
        ma.activity_month,

        dr.cohort_month,
        dr.signup_date,
        dr.segment,
        dr.account_status,

        ma.total_rides,
        ma.completed_rides,
        ma.cancelled_rides,
        ma.promo_rides,
        ma.surge_rides,
        ma.cities_ridden,
        ma.service_types_used,

        ma.total_spend,
        ma.avg_fare,
        ma.total_distance_km,
        ma.avg_trip_duration_min,

        SAFE_DIVIDE(ma.completed_rides, ma.total_rides)         as completion_rate,
        SAFE_DIVIDE(ma.cancelled_rides, ma.total_rides)         as cancellation_rate,

        DATE_DIFF(ma.activity_month, dr.cohort_month, MONTH)    as months_since_signup

    from monthly_agg   ma
    left join dim_rider dr on ma.rider_id = dr.rider_id

)

select * from final
