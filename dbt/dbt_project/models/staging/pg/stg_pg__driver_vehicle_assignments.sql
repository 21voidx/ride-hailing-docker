with source as (
    select * from {{ source('dev_bronze_pg', 'driver_vehicle_assignment') }}
),

final as (
    select
        assignment_id,
        driver_id,
        vehicle_id,
        assigned_from,
        assigned_to,
        is_active,
        created_at,
        updated_at,
        (is_active and (assigned_to is null or assigned_to > CURRENT_TIMESTAMP()))
                                                                                as is_currently_active
    from source
)

select * from final
