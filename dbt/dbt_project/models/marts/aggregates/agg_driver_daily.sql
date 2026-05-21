{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'activity_date',
            'data_type': 'date'
        },
        cluster_by=['driver_id']
    )
}}

with fct_rides as (

    select * from {{ ref('fct_rides') }}

),

dim_driver as (

    select
        driver_id,
        user_id,
        driver_status,
        verification_status,
        all_docs_verified,
        active_vehicle_type,
        rating_avg,
        rating_count

    from {{ ref('dim_driver') }}

),

fct_reviews as (

    select
        ride_id,
        rating_score,
        review_type,
        is_positive

    from {{ ref('fct_reviews') }}
    where review_type = 'RIDER_TO_DRIVER'

),

ride_agg as (

    select
        r.driver_id,
        r.ride_date                                           as activity_date,

        COUNT(r.ride_id)                                      as total_rides,
        COUNTIF(r.is_completed)                               as completed_rides,
        COUNTIF(r.is_cancelled_by_driver)                     as driver_cancellations,
        COUNTIF(r.has_surge)                                  as surge_rides,

        SUM(case when r.is_completed then r.driver_earning else 0 end) as total_earnings,
        SUM(case when r.is_completed then r.distance_km else 0 end)    as total_distance_km,
        AVG(case when r.is_completed then r.duration_minutes end)      as avg_trip_duration_min,

        COUNT(DISTINCT r.city_code)                           as cities_active,
        COUNT(DISTINCT r.service_type)                        as service_types_active

    from fct_rides r
    group by 1, 2

),

review_agg as (

    select
        fr.driver_id,
        r.ride_date                                           as activity_date,
        AVG(rv.rating_score)                                  as daily_avg_rating,
        COUNT(rv.review_type)                                 as reviews_received,
        COUNTIF(rv.is_positive)                               as positive_reviews

    from fct_rides fr
    join fct_reviews rv on fr.ride_id = rv.ride_id
    left join fct_rides r on rv.ride_id = r.ride_id
    group by 1, 2

),

final as (

    select
        ra.driver_id,
        ra.activity_date,

        dd.driver_status,
        dd.verification_status,
        dd.all_docs_verified,
        dd.active_vehicle_type,
        dd.rating_avg                                         as lifetime_rating_avg,
        dd.rating_count                                       as lifetime_rating_count,

        ra.total_rides,
        ra.completed_rides,
        ra.driver_cancellations,
        ra.surge_rides,
        ra.total_earnings,
        ra.total_distance_km,
        ra.avg_trip_duration_min,
        ra.cities_active,
        ra.service_types_active,

        IFNULL(rev.daily_avg_rating, 0)                       as daily_avg_rating,
        IFNULL(rev.reviews_received, 0)                       as reviews_received,
        IFNULL(rev.positive_reviews, 0)                       as positive_reviews,

        SAFE_DIVIDE(COUNTIF(ra.completed_rides > 0), 1)       as online_completion_rate

    from ride_agg         ra
    left join dim_driver   dd  on ra.driver_id = dd.driver_id
    left join review_agg   rev on ra.driver_id = rev.driver_id and ra.activity_date = rev.activity_date

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

)

select * from final
