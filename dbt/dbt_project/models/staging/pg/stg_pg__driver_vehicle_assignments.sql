with source as (
    select
        assignment_id,
driver_id,
vehicle_id,
assigned_from,
assigned_to,
is_active,
created_at,
updated_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'driver_vehicle_assignment') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by assignment_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
