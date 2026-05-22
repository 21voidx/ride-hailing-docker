{{ config(materialized='view') }}

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
    created_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__ride_fares') }}
where fare_type = 'FINAL'
qualify row_number() over (partition by ride_id order by fare_version desc, calculated_at desc, fare_id desc) = 1
