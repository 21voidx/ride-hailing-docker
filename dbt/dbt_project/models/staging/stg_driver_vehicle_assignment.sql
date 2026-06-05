with src as (select * from {{ ref('snap_driver_vehicle_assignment') }})
select
    cast(dbt_scd_id as string) as assignment_snapshot_id,
    cast(assignment_id as int64) as assignment_id,
    cast(driver_id as int64) as driver_id,
    cast(vehicle_id as int64) as vehicle_id,
    cast(assigned_from as timestamp) as assigned_from,
    cast(assigned_to as timestamp) as assigned_to,
    cast(is_active as bool) as is_active,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current
from src
