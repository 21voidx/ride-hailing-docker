{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    p.payment_date as metric_date,
    coalesce(pm.method_code, 'UNKNOWN') as method_code,
    p.provider_name,
    count(*) as payment_attempts,
    countif(p.payment_status = 'PAID') as paid_payments,
    countif(p.payment_status = 'FAILED') as failed_payments,
    {{ safe_divide("countif(p.payment_status = 'PAID')", 'count(*)') }} as payment_success_rate,
    sum(p.amount) as payment_amount,
    sum(p.method_fee) as method_fee,
    current_timestamp() as _dbt_loaded_at
from {{ ref('fct_payments') }} p
left join {{ ref('dim_payment_method') }} pm using (payment_method_sk)
{% if is_incremental() %}
where p.payment_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
{% endif %}
group by 1, 2, 3
