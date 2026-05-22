{{ config(
    unique_key=['requested_date', 'city_code', 'service_type', 'ride_status'],
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['city_code', 'service_type', 'ride_status']
) }}

select
    requested_date,
    city_code,
    service_type,
    ride_status,
    requested_rides,
    completed_rides,
    cancelled_rides,
    ride_payment_failed_count,
    paid_rides,
    payment_failed_count,
    total_fare,
    platform_fee,
    driver_earning,
    discount_amount,
    avg_total_fare,
    {{ safe_divide('completed_rides', 'requested_rides') }} as completion_rate,
    {{ safe_divide('cancelled_rides', 'requested_rides') }} as cancellation_rate,
    {{ safe_divide('payment_failed_count', 'requested_rides') }} as payment_failure_rate,
    {{ audit_columns() }}
from {{ ref('int_metrics__ride_daily_city_service') }}
{% if is_incremental() %}
where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
