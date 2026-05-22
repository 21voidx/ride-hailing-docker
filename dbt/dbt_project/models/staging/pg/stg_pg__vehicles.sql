with source as (
    select * from {{ source('dev_bronze_pg', 'vehicle') }}
),

final as (
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
        vehicle_status = 'ACTIVE'               as is_active,
        verified_at,
        created_at,
        updated_at,
        deleted_at
    from source
)

select * from final
