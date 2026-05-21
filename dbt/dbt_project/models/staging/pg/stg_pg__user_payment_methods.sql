with source as (

    select * from {{ source('bronze_pg', 'user_payment_method') }}

),

renamed as (

    select
        user_payment_method_id,
        user_id,
        payment_method_type_id,
        provider_name,
        -- provider_customer_id intentionally excluded (PII / sensitive)
        -- provider_payment_token intentionally excluded (PII / sensitive)
        masked_account,
        expiry_month,
        expiry_year,
        is_default,
        payment_method_status,
        created_at,
        updated_at,
        deleted_at,
        _ingested_at,
        _source_system

    from source
    where deleted_at is null

)

select * from renamed
