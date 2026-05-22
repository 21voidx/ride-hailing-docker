-- Append-only CDC event log for payment transactions.
-- JANGAN filter __op = 'd' — log harus utuh.
with source as (
    select *
    from {{ source('dev_bronze_cdc_events', 'payment_transaction_events') }}
    where {{ cdc_partition_filter(days_back=60) }}
),

final as (
    select
        cast(transaction_id as INT64)                           as transaction_id,
        cast(ride_id as INT64)                                  as ride_id,
        cast(user_payment_method_id as INT64)                   as user_payment_method_id,
        provider_name,
        provider_transaction_id,
        idempotency_key,
        cast(amount as NUMERIC)                                 as amount,
        cast(method_fee as NUMERIC)                             as method_fee,
        currency_code,
        payment_status,
        failure_code,
        failure_message,
        {{ cast_debezium_timestamp('authorized_at') }}          as authorized_at,
        {{ cast_debezium_timestamp('captured_at') }}            as captured_at,
        {{ cast_debezium_timestamp('paid_at') }}                as paid_at,
        {{ cast_debezium_timestamp('created_at') }}             as created_at,
        {{ cast_debezium_timestamp('updated_at') }}             as updated_at,
        __op,
        __table,
        cast(__lsn as INT64)                                    as __lsn,
        cast(__source_ts_ms as INT64)                           as __source_ts_ms
    from source
)

select * from final
