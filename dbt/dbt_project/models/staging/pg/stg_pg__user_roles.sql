with source as (

    select * from {{ source('bronze_pg', 'user_role') }}

),

renamed as (

    select
        user_id,
        role_id,
        assigned_at,
        assigned_by,
        is_active,
        _ingested_at,
        _source_system

    from source

)

select * from renamed
