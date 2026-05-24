-- Staging: Driver Vehicle Assignment
-- Source    : Batch → dev_bronze_pg.driver_vehicle_assignment
-- Strategy  : Upsert. Bisa ada multiple rows per driver (histori assignment).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'driver_vehicle_assignment') }}
),

renamed as (
    select
        assignment_id,
        driver_id,
        vehicle_id,
        assigned_from,
        assigned_to,
        is_active,
        created_at,
        updated_at

    from source
    where assignment_id is not null
)

select * from renamed
