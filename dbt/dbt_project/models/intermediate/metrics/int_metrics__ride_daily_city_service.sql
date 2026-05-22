{{ config(
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['city_code', 'service_type', 'ride_status']
) }}

with rides as (
    select *
    from {{ ref('int_core__rides_unified') }}
    {% if is_incremental() %}
      where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
    {% endif %}
), fares as (
    select * from {{ ref('int_core__final_fares') }}
), payments as (
    select * from {{ ref('int_core__payment_status_per_ride') }}
)
select
    {{ surrogate_key(["'ride_daily_city_service'", 'r.requested_date', 'r.city_code', 'r.service_type', 'r.ride_status']) }} as ride_daily_city_service_key,
    r.requested_date,
    r.requested_date_key,
    r.city_code,
    r.service_type,
    r.ride_status,
    count(*) as requested_rides,
    countif(r.is_completed) as completed_rides,
    countif(r.is_cancelled) as cancelled_rides,
    countif(r.is_payment_failed) as ride_payment_failed_count,
    countif(p.is_paid) as paid_rides,
    countif(p.is_payment_failed) as payment_failed_count,
    sum(coalesce(f.total_fare, 0)) as total_fare,
    sum(coalesce(f.platform_fee, 0)) as platform_fee,
    sum(coalesce(f.driver_earning, 0)) as driver_earning,
    sum(coalesce(f.discount_amount, 0)) as discount_amount,
    avg(f.total_fare) as avg_total_fare,
    {{ audit_columns() }}
from rides r
left join fares f on r.ride_id = f.ride_id
left join payments p on r.ride_id = p.ride_id
group by 1,2,3,4,5,6
