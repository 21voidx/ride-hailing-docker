{{ config(materialized='view') }}

select
    transaction_id,
    ride_id,
    user_payment_method_id,
    provider_name,
    provider_transaction_id,
    idempotency_key,
    amount,
    method_fee,
    currency_code,
    payment_status,
    failure_code,
    failure_message,
    authorized_at,
    captured_at,
    paid_at,
    created_at,
    updated_at,
    cdc_event_at,
    {{ is_status('payment_status', 'PAID') }} as is_paid,
    {{ is_status('payment_status', 'FAILED') }} as is_payment_failed,
    {{ audit_columns() }}
from {{ ref('int_cdc__payment_transactions_latest') }}
where not is_deleted
qualify row_number() over (partition by ride_id order by coalesce(paid_at, updated_at, created_at, cdc_event_at) desc, transaction_id desc) = 1
