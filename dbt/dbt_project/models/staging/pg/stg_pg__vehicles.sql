with source as (
    select
        vehicle_id,
license_plate,
vehicle_make,
vehicle_model,
vehicle_year,
vehicle_capacity,
vehicle_color,
vehicle_type,
vehicle_status,
verified_at,
created_at,
updated_at,
deleted_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'vehicle') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by vehicle_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
