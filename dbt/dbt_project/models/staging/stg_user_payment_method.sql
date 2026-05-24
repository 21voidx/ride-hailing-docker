-- Staging: User Payment Method
-- Source    : Batch → dev_bronze_pg.user_payment_method
-- Strategy  : Upsert. Satu baris per user_payment_method_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).
--             provider_customer_id tidak diekspos di staging (PII/sensitive).

with source as (
    select * from {{ source('ride_ops_batch', 'user_payment_method') }}
),

renamed as (
    select
        user_payment_method_id,
        user_id,
        payment_method_type_id,
        provider_name,
        -- provider_customer_id dikecualikan (PII)
        masked_account,
        expiry_month,
        expiry_year,
        is_default,
        payment_method_status,
        created_at,
        updated_at,
        deleted_at,

        -- derived
        payment_method_status = 'ACTIVE' as is_active,
        deleted_at is not null           as is_deleted

    from source
    where user_payment_method_id is not null
)

select * from renamed
