{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    refund_date as metric_date,
    refund_status,
    refund_reason_code,
    count(*) as refund_count,
    sum(refund_amount) as refund_amount,
    avg(refund_processing_min) as avg_refund_processing_min,
    current_timestamp() as _dbt_loaded_at
from {{ ref('fct_refunds') }}
{% if is_incremental() %}
where refund_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
{% endif %}
group by 1, 2, 3
