-- Sensitive columns provider_customer_id and provider_payment_token are excluded.
with source as (
    select * from {{ source('dev_bronze_pg', 'user_payment_method') }}
),

final as (
    select
        user_payment_method_id,
        user_id,
        payment_method_type_id,
        provider_name,
        -- provider_customer_id excluded
        -- provider_payment_token excluded
        masked_account,
        expiry_month,
        expiry_year,
        is_default,
        payment_method_status,
        created_at,
        updated_at,
        deleted_at
    from source
)

select * from final
