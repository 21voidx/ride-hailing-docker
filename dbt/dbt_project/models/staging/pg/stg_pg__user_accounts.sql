with source as (
    select
        user_id,
username,
to_hex(sha256(cast(lower(trim(email)) as bytes))) as email_hash,
to_hex(sha256(cast(regexp_replace(coalesce(phone_number, ''), r'\D', '') as bytes))) as phone_hash,
account_status,
email_verified_at,
phone_verified_at,
last_login_at,
created_at,
updated_at,
deleted_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'user_account') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by user_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
