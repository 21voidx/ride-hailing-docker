{{
    config(
        materialized='table'
    )
}}

with rides as (
    select * from {{ ref('int__ride_enriched') }}
),

agg as (
    select
        rider_id                                                        as user_id,
        COUNT(*)                                                        as total_rides,
        COUNTIF(is_completed)                                           as completed_rides,
        COUNTIF(is_cancelled)                                           as cancelled_rides,
        COALESCE(SUM(case when is_completed then total_fare end), 0)    as total_spend,
        MIN(requested_at)                                               as first_ride_at,
        MAX(requested_at)                                               as last_ride_at,
        DATE_DIFF(
            CURRENT_DATE(),
            DATE(MAX(requested_at), 'Asia/Jakarta'),
            DAY
        )                                                               as days_since_last_ride,
        SAFE_DIVIDE(
            SUM(case when is_completed then total_fare end),
            NULLIF(COUNTIF(is_completed), 0)
        )                                                               as avg_fare,
        (
            select service_type
            from (
                select service_type, COUNT(*) as cnt
                from rides r2
                where r2.rider_id = rides.rider_id and r2.is_completed
                group by service_type
                order by cnt desc
                limit 1
            )
        )                                                               as favorite_service_type
    from rides
    group by rider_id
)

select * from agg
