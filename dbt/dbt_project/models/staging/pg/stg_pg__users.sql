with source as (
    select * from {{ source('dev_bronze_pg', 'user_account') }}
),

final as (
    select
        user_id,
        username,
        email,
        phone_number,
        -- password_hash intentionally excluded
        account_status,
        email_verified_at,
        phone_verified_at,
        last_login_at,
        created_at,
        updated_at,
        deleted_at,
        {{ get_jakarta_date('created_at') }}     as signup_date
    from source
)

select * from final
