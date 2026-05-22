{{ config(
    unique_key=['requested_date', 'driver_id', 'service_type'],
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['driver_id', 'service_type']
) }}

select
    requested_date,
    driver_id,
    service_type,
    assigned_rides,
    completed_rides,
    cancelled_rides,
    total_fare,
    platform_fee,
    driver_earning,
    avg_accept_minutes,
    avg_trip_minutes,
    avg_rating_score,
    reviewed_rides,
    {{ safe_divide('completed_rides', 'assigned_rides') }} as completion_rate,
    {{ safe_divide('cancelled_rides', 'assigned_rides') }} as cancellation_rate,
    {{ audit_columns() }}
from {{ ref('int_metrics__driver_daily') }}
{% if is_incremental() %}
where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
