{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns'
    )
}}

with transactions as (

    select * from {{ ref('stg_cdc__payment_transactions') }}

    {% if is_incremental() %}
    where __source_ts_ms > (
        select coalesce(MAX(__source_ts_ms), 0)
        from {{ this }}
    )
    {% endif %}

),

refund_agg as (

    select
        transaction_id,
        SUM(case when is_completed then refund_amount else 0 end) as total_refunded,
        COUNT(*)                                                   as refund_count
    from {{ ref('stg_pg__payment_refunds') }}
    group by 1

),

user_payment_methods as (

    select * from {{ ref('stg_pg__user_payment_methods') }}

),

method_types as (

    select * from {{ ref('stg_pg__payment_method_types') }}

),

enriched as (

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

        {{ get_jakarta_date('t.created_at') }}              as txn_date,

        IFNULL(ra.total_refunded, 0)                        as total_refunded,
        IFNULL(ra.refund_count, 0)                          as refund_count,
        t.amount - IFNULL(ra.total_refunded, 0)             as net_amount,

        upm.user_id,
        upm.payment_method_type_id,
        upm.masked_account,
        upm.is_default                                      as is_default_payment_method,
        upm.payment_method_status,

        mt.method_code,
        mt.method_name,

        t.__source_ts_ms,
        t.__op,
        t.__lsn

    from transactions    t
    left join refund_agg ra  on t.transaction_id           = ra.transaction_id
    left join user_payment_methods upm on t.user_payment_method_id = upm.user_payment_method_id
    left join method_types mt           on upm.payment_method_type_id = mt.payment_method_type_id

)

select * from enriched
