{{ config(
    unique_key=['requested_date', 'city_code', 'service_type'],
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['city_code', 'service_type']
) }}

with base as (
    select *
    from {{ ref('rpt_looker__ride_operations_dashboard') }}
    {% if is_incremental() %}
      where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
    {% endif %}
)
select
    {{ surrogate_key(["'daily_kpi'", 'requested_date', 'city_code', 'service_type']) }} as daily_kpi_key,
    requested_date,
    requested_date_key,
    city_code,
    service_type,
    count(*) as requested_rides,
    countif(is_completed) as completed_rides,
    countif(is_cancelled) as cancelled_rides,
    countif(is_ride_payment_failed) as ride_payment_failed_count,
    countif(is_payment_transaction_failed) as payment_failed_count,
    countif(is_paid) as paid_rides,
    countif(is_promo_used) as promo_used_rides,
    sum(coalesce(total_fare, 0)) as gross_booking_value,
    sum(coalesce(platform_fee, 0)) as platform_revenue,
    sum(coalesce(driver_earning, 0)) as driver_earnings,
    sum(coalesce(discount_amount, 0)) as total_discount,
    avg(total_fare) as avg_fare,
    avg(request_to_accept_minutes) as avg_accept_minutes,
    avg(pickup_wait_minutes) as avg_pickup_wait_minutes,
    avg(trip_minutes) as avg_trip_minutes,
    avg(rider_to_driver_rating_score) as avg_rating,
    {{ safe_divide('countif(is_completed)', 'count(*)') }} as completion_rate,
    {{ safe_divide('countif(is_cancelled)', 'count(*)') }} as cancellation_rate,
    {{ safe_divide('countif(is_payment_transaction_failed)', 'count(*)') }} as payment_failure_rate,
    {{ safe_divide('countif(is_promo_used)', 'count(*)') }} as promo_usage_rate,
    {{ audit_columns() }}
from base
group by 1,2,3,4,5
