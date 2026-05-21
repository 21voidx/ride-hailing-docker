with source as (

    select * from {{ source('bronze_pg', 'vehicle') }}

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

        (vehicle_status = 'ACTIVE')  as is_active,

        _ingested_at,
        _source_system

    from source

)

select * from renamed
