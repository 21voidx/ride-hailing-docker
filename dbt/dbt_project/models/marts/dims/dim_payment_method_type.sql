with source as (
    select * from {{ ref('stg_pg__payment_method_types') }}
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
