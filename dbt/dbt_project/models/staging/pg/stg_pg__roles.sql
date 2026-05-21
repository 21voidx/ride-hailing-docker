with source as (

    select * from {{ source('bronze_pg', 'role') }}

),

renamed as (

    select
        role_id,
        role_code,
        role_name,
        description,
        is_active,
        created_at,
        updated_at,
        _ingested_at,
        _source_system

    from source

)

select * from renamed
