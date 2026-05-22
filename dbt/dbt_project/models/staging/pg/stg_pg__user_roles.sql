with source as (
    select * from {{ source('dev_bronze_pg', 'user_role') }}
),

final as (
    select
        user_id,
        role_id,
        assigned_at,
        assigned_by,
        is_active
    from source
)

select * from final
