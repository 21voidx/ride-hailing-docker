-- Staging: Vehicle
-- Source    : Batch → dev_bronze_pg.vehicle
-- Strategy  : Upsert. Satu baris per vehicle_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'vehicle') }}
),

renamed as (
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

        -- derived
        deleted_at is not null    as is_deleted,
        vehicle_status = 'ACTIVE' as is_active,
        concat(
            vehicle_make, ' ', vehicle_model, ' ', cast(vehicle_year as string)
        )                         as vehicle_full_name

    from source
    where vehicle_id is not null
)

select * from renamed
