-- Staging: Payment Refund
-- Source    : Batch → dev_bronze_pg.payment_refund
-- Strategy  : Upsert. Satu baris per refund_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'payment_refund') }}
),

renamed as (
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

        -- derived
        refund_status = 'COMPLETED' as is_completed,
        timestamp_diff(completed_at, requested_at, second) as resolution_seconds

    from source
    where refund_id is not null
)

select * from renamed
