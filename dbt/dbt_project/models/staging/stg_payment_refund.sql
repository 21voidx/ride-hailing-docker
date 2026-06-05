select
    cast(refund_id as int64) as refund_id,
    cast(payment_transaction_id as int64) as payment_transaction_id,
    cast(ride_id as int64) as ride_id,
    cast(refund_amount as numeric) as refund_amount,
    upper(cast(refund_reason_code as string)) as refund_reason_code,
    upper(cast(refund_status as string)) as refund_status,
    cast(requested_at as timestamp) as requested_at,
    cast(processed_at as timestamp) as processed_at,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('bronze_mysql', 'payment_refund') }}
