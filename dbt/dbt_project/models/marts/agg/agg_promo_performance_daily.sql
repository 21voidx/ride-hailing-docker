{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'metric_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    requested_date as metric_date,
    promotion_sk,
    city_code,
    service_type,
    count(*) as promo_rides,
    countif(is_completed) as completed_promo_rides,
    sum(coalesce(promo_discount_amount, 0)) as promo_discount_amount,
    sum(coalesce(total_fare, 0)) as gross_revenue_after_discount,
    {{ safe_divide('sum(coalesce(total_fare, 0))', 'nullif(sum(coalesce(promo_discount_amount, 0)), 0)') }} as revenue_per_promo_discount,
    avg(rating_score) as avg_rating,
    current_timestamp() as _dbt_loaded_at
from {{ ref('fct_rides') }}
where promotion_sk is not null
{% if is_incremental() %}
  and requested_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
{% endif %}
group by 1, 2, 3, 4
