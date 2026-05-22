with source as (
    select * from {{ source('dev_bronze_pg', 'ride_fare_component') }}
),

final as (
    select
        fare_component_id,
        fare_id,
        component_code,
        component_name,
        component_amount,
        description,
        created_at
    from source
)

select * from final
