-- Staging: User Account
-- Source    : Batch → dev_bronze_pg.user_account
-- Strategy  : Upsert. Satu baris per user_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'user_account') }}
),

renamed as (
    select
        user_id,
        username,
        email,
        phone_number,
        account_status,
        email_verified_at,
        phone_verified_at,
        last_login_at,
        created_at,
        updated_at,
        deleted_at,

        -- derived
        deleted_at is not null        as is_deleted,
        email_verified_at is not null as is_email_verified,
        phone_verified_at is not null as is_phone_verified

    from source
    where user_id is not null
)

select * from renamed
