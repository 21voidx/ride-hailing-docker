with source as (
    select
        refund_id,
transaction_id,
provider_refund_id,
refund_amount,
currency_code,
refund_status,
refund_reason_code,
refund_reason_note,
requested_at,
completed_at,
created_at,
updated_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'payment_refund') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by refund_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
