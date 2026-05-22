with source as (
    select * from {{ source('dev_bronze_pg', 'payment_method_type') }}
),

final as (
    select
        payment_method_type_id,
        method_code,
        method_name,
        is_active,
        created_at,
        updated_at
    from source
)

select * from final
