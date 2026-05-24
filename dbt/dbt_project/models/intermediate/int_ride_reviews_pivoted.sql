-- Intermediate: pivot review per ride
-- Grain: 1 row per ride_id

with reviews as (
    select * from {{ ref('stg_review') }}
),

aggregated as (
    select
        ride_id,
        max(case when review_type = 'RIDER_TO_DRIVER' then rating_score end) as rider_rating_given,
        max(case when review_type = 'DRIVER_TO_RIDER' then rating_score end) as driver_rating_given,
        countif(review_type = 'RIDER_TO_DRIVER') > 0 as has_rider_review,
        countif(review_type = 'DRIVER_TO_RIDER') > 0 as has_driver_review,
        max(updated_at) as latest_review_updated_at
    from reviews
    group by ride_id
)

select * from aggregated
