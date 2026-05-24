-- Staging: Payment Transaction
-- Source    : CDC → dev_bronze_cdc_events.payment_transaction_events
-- Strategy  : Ambil state terbaru per transaction_id (dedup via __lsn DESC)
--             Filter __op = 'd' (deleted records)
-- Notes     : Timestamps dari Debezium disimpan sebagai STRING ISO 8601.
--             Partition filter wajib karena require_partition_filter = TRUE.

with source as (
    select * from {{ source('ride_ops_cdc', 'payment_transaction_events') }}
    -- Partition filter wajib: cover semua data historis
    where _PARTITIONTIME >= TIMESTAMP('{{ var("cdc_partition_start", "2020-01-01") }}')
),

-- Ambil 1 event terbaru per transaction_id berdasarkan LSN tertinggi
deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by __lsn desc, __source_ts_ms desc
    ) = 1
),

-- Hanya proses record yang tidak di-delete
active as (
    select * from deduplicated
    where __op != 'd'
),

renamed as (
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

        -- timestamps: cast dari STRING (Debezium ISO 8601) ke TIMESTAMP
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', authorized_at) as authorized_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', captured_at)   as captured_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', paid_at)       as paid_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at)    as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', updated_at)    as updated_at,

        -- derived
        payment_status = 'PAID'     as is_paid,
        payment_status = 'FAILED'   as is_failed,
        payment_status = 'REFUNDED' as is_refunded,
        amount + method_fee         as total_charged,

        -- CDC metadata (audit)
        __op                              as _cdc_op,
        __lsn                             as _cdc_lsn,
        timestamp_millis(__source_ts_ms)  as _cdc_source_ts

    from active
    where transaction_id is not null
)

select * from renamed
