with source as (

    select * from {{ source('bronze_pg', 'ride_fare_component') }}

),

renamed as (

    select
        fare_component_id,
        fare_id,
        component_code,
        component_name,
        component_amount,
        description,
        created_at,
        _ingested_at,
        _source_system

    from source

)

select * from renamed
