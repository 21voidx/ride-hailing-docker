{{
    config(
        materialized='table',
        partition_by={'field': 'activity_month', 'data_type': 'date'}
    )
}}

with rides as (
    select * from {{ ref('fct_rides') }}
),

riders as (
    select user_id, segment, cohort_month
    from {{ ref('dim_rider') }}
),

agg as (
    select
        r.rider_id                                          as user_id,
        DATE_TRUNC(r.ride_date, MONTH)                      as activity_month,
        COUNT(*)                                            as monthly_rides,
        COUNTIF(r.is_completed)                             as monthly_completed_rides,
        COUNTIF(r.is_cancelled)                             as monthly_cancelled_rides,
        COALESCE(SUM(case when r.is_completed then r.total_fare end), 0)
                                                            as monthly_spend,
        COALESCE(AVG(case when r.is_completed then r.total_fare end), 0)
                                                            as avg_fare,
        COUNTIF(r.has_promo)                                as promo_rides,
        COUNTIF(r.has_surge)                                as surge_rides,
        ARRAY_AGG(DISTINCT r.service_type IGNORE NULLS)     as service_types_used
    from rides r
    group by r.rider_id, DATE_TRUNC(r.ride_date, MONTH)
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'activity_month']) }}
                                                            as agg_key,
    a.*,
    d.segment,
    d.cohort_month
from agg a
left join riders d on a.user_id = d.user_id
