with source as (

    select * from {{ source('bronze_cdc_current', 'payment_transaction') }}

),

cast_and_clean as (

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

        {{ cast_debezium_timestamp('authorized_at') }} as authorized_at,
        {{ cast_debezium_timestamp('captured_at') }}   as captured_at,
        {{ cast_debezium_timestamp('paid_at') }}       as paid_at,
        {{ cast_debezium_timestamp('created_at') }}    as created_at,
        {{ cast_debezium_timestamp('updated_at') }}    as updated_at,

        (payment_status = 'PAID')                      as is_paid,
        (payment_status = 'FAILED')                    as is_failed,
        (payment_status = 'REFUNDED')                  as is_refunded,

        __op,
        __table,
        __lsn,
        __source_ts_ms

    from source
    where coalesce(cast(__op as string), '') != 'd'

)

select * from cast_and_clean
