{{
    config(
        materialized='table',
        partition_by={'field': 'activity_date', 'data_type': 'date'}
    )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
    where driver_id is not null
),

reviews as (
    select
        reviewee_id         as driver_user_id,
        review_date,
        rating_score,
        is_positive
    from {{ ref('fct_reviews') }}
    where review_type = 'RIDER_TO_DRIVER'
),

drivers as (
    select driver_id, user_id
    from {{ ref('dim_driver') }}
),

agg_rides as (
    select
        r.driver_id,
        r.ride_date                                 as activity_date,
        COUNT(*)                                    as total_trips,
        COUNTIF(r.is_completed)                     as completed_trips,
        COUNTIF(r.is_cancelled)                     as cancelled_trips,
        COALESCE(SUM(r.driver_earning), 0)          as total_earning,
        COALESCE(SUM(r.distance_km), 0)             as total_distance_km,
        COALESCE(AVG(r.duration_minutes), 0)        as avg_trip_duration_min,
        COUNTIF(r.has_surge)                        as surge_trips
    from rides r
    group by r.driver_id, r.ride_date
),

agg_reviews as (
    select
        d.driver_id,
        rv.review_date                              as activity_date,
        COUNT(*)                                    as reviews_received,
        COALESCE(AVG(rv.rating_score), 0)           as avg_rating_day,
        COUNTIF(rv.is_positive)                     as positive_reviews
    from reviews rv
    join drivers d on rv.driver_user_id = d.user_id
    group by d.driver_id, rv.review_date
),

final as (
    select
        ar.driver_id,
        ar.activity_date,
        ar.total_trips,
        ar.completed_trips,
        ar.cancelled_trips,
        ar.total_earning,
        ar.total_distance_km,
        ar.avg_trip_duration_min,
        ar.surge_trips,
        COALESCE(rv.reviews_received, 0)            as reviews_received,
        COALESCE(rv.avg_rating_day, 0)              as avg_rating_day,
        COALESCE(rv.positive_reviews, 0)            as positive_reviews
    from agg_rides ar
    left join agg_reviews rv
           on ar.driver_id = rv.driver_id
          and ar.activity_date = rv.activity_date
)

select
    {{ dbt_utils.generate_surrogate_key(['driver_id', 'activity_date']) }}
                                                    as agg_key,
    *
from final
