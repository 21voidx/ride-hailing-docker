{{ config(
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['driver_id', 'service_type']
) }}

with rides as (
    select *
    from {{ ref('int_core__rides_unified') }}
    {% if is_incremental() %}
      where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
    {% endif %}
), lifecycle as (
    select * from {{ ref('int_core__ride_lifecycle') }}
), fares as (
    select * from {{ ref('int_core__final_fares') }}
), reviews as (
    select * from {{ ref('int_core__reviews_per_ride') }}
)
select
    r.requested_date,
    r.driver_id,
    r.service_type,
    count(*) as assigned_rides,
    countif(r.is_completed) as completed_rides,
    countif(r.is_cancelled) as cancelled_rides,
    sum(coalesce(f.total_fare, 0)) as total_fare,
    sum(coalesce(f.platform_fee, 0)) as platform_fee,
    sum(coalesce(f.driver_earning, 0)) as driver_earning,
    avg(l.request_to_accept_minutes) as avg_accept_minutes,
    avg(l.trip_minutes) as avg_trip_minutes,
    avg(reviews.rider_to_driver_rating_score) as avg_rating_score,
    count(reviews.ride_id) as reviewed_rides,
    {{ audit_columns() }}
from rides r
left join lifecycle l on r.ride_id = l.ride_id
left join fares f on r.ride_id = f.ride_id
left join reviews on r.ride_id = reviews.ride_id
where r.driver_id is not null
group by 1,2,3
