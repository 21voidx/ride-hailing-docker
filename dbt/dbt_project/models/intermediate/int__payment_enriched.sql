{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='append_new_columns'
    )
}}

with txns as (
    select *
    from {{ ref('stg_cdc__payment_transactions') }}
    {% if is_incremental() %}
    where TIMESTAMP_MILLIS(__source_ts_ms) > (
        select TIMESTAMP_SUB(MAX(TIMESTAMP_MILLIS(__source_ts_ms)), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

refunds_agg as (
    select
        transaction_id,
        COALESCE(SUM(case when is_completed then refund_amount end), 0) as total_refunded,
        COUNT(*)                                                        as refund_count
    from {{ ref('stg_pg__payment_refunds') }}
    group by transaction_id
),

upm as (
    select * from {{ ref('stg_pg__user_payment_methods') }}
),

pmt as (
    select * from {{ ref('stg_pg__payment_method_types') }}
),

final as (
    select
        t.transaction_id,
        t.ride_id,
        t.user_payment_method_id,
        t.provider_name,
        t.provider_transaction_id,
        t.idempotency_key,
        t.amount,
        t.method_fee,
        t.currency_code,
        t.payment_status,
        t.failure_code,
        t.failure_message,
        t.authorized_at,
        t.captured_at,
        t.paid_at,
        t.created_at,
        t.updated_at,
        t.is_paid,
        t.is_failed,
        t.is_refunded,
        t.__op,
        t.__lsn,
        t.__source_ts_ms,

        -- date dimension
        {{ get_jakarta_date('t.created_at') }}          as txn_date,

        -- refund aggregates
        COALESCE(r.total_refunded, 0)                   as total_refunded,
        COALESCE(r.refund_count, 0)                     as refund_count,
        t.amount - COALESCE(r.total_refunded, 0)        as net_amount,

        -- payment method details
        upm.payment_method_type_id,
        upm.provider_name                               as method_provider_name,
        upm.masked_account,
        upm.is_default,
        upm.payment_method_status,

        -- method type details
        pmt.method_code,
        pmt.method_name                                 as method_type_name

    from txns t
    left join refunds_agg r  on t.transaction_id = r.transaction_id
    left join upm            on t.user_payment_method_id = upm.user_payment_method_id
    left join pmt            on upm.payment_method_type_id = pmt.payment_method_type_id
)

select * from final
