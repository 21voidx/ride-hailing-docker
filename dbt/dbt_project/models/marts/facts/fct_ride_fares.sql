{{ config(
    unique_key='fare_id',
    partition_by={'field': 'calculated_date', 'data_type': 'date'},
    cluster_by=['ride_id', 'fare_type', 'currency_code']
) }}

select
    fare_id,
    ride_id,
    fare_type,
    fare_version,
    currency_code,
    distance_km,
    duration_min,
    base_fare,
    distance_fare,
    time_fare,
    surge_multiplier,
    surge_amount,
    discount_amount,
    tax_amount,
    platform_fee,
    driver_earning,
    total_fare,
    fare_rule_code,
    calculated_at,
    date(calculated_at, '{{ var("timezone", "Asia/Jakarta") }}') as calculated_date,
    created_at,
    {{ audit_columns() }}
from {{ ref('int_core__final_fares') }}
{% if is_incremental() %}
where date(calculated_at, '{{ var("timezone", "Asia/Jakarta") }}') >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
