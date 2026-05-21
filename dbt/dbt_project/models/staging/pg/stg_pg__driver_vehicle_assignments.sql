with source as (

    select * from {{ source('bronze_pg', 'driver_vehicle_assignment') }}

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
        updated_at,

        (is_active = true and assigned_to is null) as is_currently_active,

        _ingested_at,
        _source_system

    from source

)

select * from renamed
