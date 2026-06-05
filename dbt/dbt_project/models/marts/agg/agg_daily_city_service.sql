{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    requested_date as metric_date,
    city_code,
    service_type,
    count(*) as requested_rides,
    countif(is_completed) as completed_rides,
    countif(is_cancelled) as cancelled_rides,
    countif(is_payment_failed) as payment_failed_rides,
    {{ safe_divide('countif(is_completed)', 'count(*)') }} as completion_rate,
    {{ safe_divide('countif(is_cancelled)', 'count(*)') }} as cancellation_rate,
    sum(coalesce(total_fare, 0)) as gross_revenue,
    sum(coalesce(platform_fee, 0)) as platform_revenue,
    sum(coalesce(driver_earning, 0)) as driver_earnings,
    avg(accept_delay_min) as avg_accept_delay_min,
    avg(driver_arrival_min) as avg_driver_arrival_min,
    avg(rating_score) as avg_rating,
    current_timestamp() as _dbt_loaded_at
from {{ ref('fct_rides') }}
{% if is_incremental() %}
where requested_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
{% endif %}
group by 1, 2, 3
