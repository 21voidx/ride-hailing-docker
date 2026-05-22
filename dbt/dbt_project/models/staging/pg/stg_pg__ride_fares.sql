with source as (
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
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'ride_fare') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by fare_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
