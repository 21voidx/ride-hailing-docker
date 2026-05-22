with source as (
    select
        user_payment_method_id,
user_id,
payment_method_type_id,
provider_name,
provider_customer_id,
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
    from {{ source('bronze_pg', 'user_payment_method') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by user_payment_method_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
