with source as (
    select * from {{ ref('stg_pg__vehicles') }}
),

final as (
    select
        vehicle_id,
        license_plate,
        vehicle_type,
        vehicle_make,
        vehicle_model,
        vehicle_year,
        vehicle_capacity,
        vehicle_color,
        vehicle_status,
        is_active,
        verified_at,
        created_at,
        updated_at,
        deleted_at
    from source
)

select * from final
