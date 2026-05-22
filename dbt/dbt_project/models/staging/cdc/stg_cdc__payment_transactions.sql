-- Current-state staging for payment transactions, built directly from partitioned CDC events.
-- This avoids querying dev_bronze_cdc_current when its underlying view does not
-- push down a required partition filter to dev_bronze_cdc_events.payment_transaction_events.

with source as (
    select *
    from {{ source('dev_bronze_cdc_events', 'payment_transaction_events') }}
    where {{ cdc_partition_filter(days_back=60) }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by cast(transaction_id as STRING)
            order by
                coalesce(safe_cast(__source_ts_ms as INT64), 0) desc,
                coalesce(safe_cast(__lsn as INT64), 0) desc,
                coalesce(
                    {{ cast_debezium_timestamp('updated_at') }},
                    {{ cast_debezium_timestamp('created_at') }}
                ) desc
        ) as rn
    from source
),

current_rows as (
    select * except (rn)
    from ranked
    where rn = 1
      and coalesce(__op, '') != 'd'
),

cast_ts as (
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
    from current_rows
),

final as (
    select
        *,
        coalesce(payment_status = 'PAID', false)                as is_paid,
        coalesce(payment_status = 'FAILED', false)              as is_failed,
        coalesce(payment_status = 'REFUNDED', false)            as is_refunded
    from cast_ts
)

select * from final