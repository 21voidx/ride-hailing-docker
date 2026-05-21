with source as (

    select * from {{ source('bronze_pg', 'payment_method_type') }}

),

renamed as (

    select
        payment_method_type_id,
        method_code,
        method_name,
        is_active,
        created_at,
        updated_at,
        _ingested_at,
        _source_system

    from source

)

select * from renamed
