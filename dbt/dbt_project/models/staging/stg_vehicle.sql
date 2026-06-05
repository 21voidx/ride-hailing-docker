with src as (select * from {{ ref('snap_vehicle') }})
select
    cast(dbt_scd_id as string) as vehicle_snapshot_id,
    cast(vehicle_id as int64) as vehicle_id,
    cast(driver_id as int64) as driver_id,
    upper(cast(license_plate as string)) as license_plate,
    upper(cast(vehicle_type as string)) as vehicle_type,
    cast(vehicle_make as string) as vehicle_make,
    cast(vehicle_model as string) as vehicle_model,
    cast(vehicle_year as int64) as vehicle_year,
    upper(cast(vehicle_status as string)) as vehicle_status,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    deleted_at is not null as is_deleted
from src
