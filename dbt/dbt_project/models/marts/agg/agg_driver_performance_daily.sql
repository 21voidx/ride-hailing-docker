{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    requested_date as metric_date,
    driver_sk,
    city_code,
    service_type,
    count(*) as assigned_rides,
    countif(is_completed) as completed_rides,
    {{ safe_divide('countif(is_completed)', 'count(*)') }} as driver_completion_rate,
    sum(coalesce(driver_earning, 0)) as driver_earning,
    avg(driver_arrival_min) as avg_arrival_min,
    avg(rating_score) as avg_rating,
    countif(priority = 'HIGH') as high_priority_tickets,
    current_timestamp() as _dbt_loaded_at
from {{ ref('fct_rides') }}
where driver_sk is not null
{% if is_incremental() %}
  and requested_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
{% endif %}
group by 1, 2, 3, 4
