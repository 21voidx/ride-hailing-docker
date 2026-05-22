with source as (
    select * from {{ source('dev_bronze_pg', 'role') }}
),

final as (
    select
        role_id,
        role_code,
        role_name,
        description,
        is_active,
        created_at,
        updated_at
    from source
)

select * from final
