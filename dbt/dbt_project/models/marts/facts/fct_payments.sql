{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='append_new_columns'
    )
}}

with payments as (
    select *
    from {{ ref('int__payment_enriched') }}
    {% if is_incremental() %}
    where TIMESTAMP_MILLIS(__source_ts_ms) > (
        select TIMESTAMP_SUB(MAX(TIMESTAMP_MILLIS(__source_ts_ms)), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

rides as (
    select ride_id, rider_id, city_code, service_type
    from {{ ref('fct_rides') }}
),

dim_rider as (
    select user_id
    from {{ ref('dim_rider') }}
),

dim_pmt as (
    select payment_method_type_id, method_code, method_name
    from {{ ref('dim_payment_method_type') }}
),

final as (
    select
        p.transaction_id,
        p.ride_id,
        r.rider_id                                                      as user_id,
        p.user_payment_method_id,
        p.payment_method_type_id,

        -- txn attributes
        p.provider_name,
        p.provider_transaction_id,
        p.idempotency_key,
        p.payment_status,
        p.failure_code,
        p.failure_message,
        p.txn_date,
        p.authorized_at,
        p.captured_at,
        p.paid_at,
        p.created_at,
        p.updated_at,

        -- method info
        p.method_code,
        p.method_type_name,
        p.masked_account,
        p.payment_method_status,

        -- measures
        p.amount,
        p.method_fee,
        p.net_amount,
        p.total_refunded,
        p.refund_count,

        -- boolean flags
        p.is_paid,
        p.is_failed,
        p.is_refunded,

        -- CDC metadata
        p.__lsn,
        p.__source_ts_ms

    from payments p
    left join rides r         on p.ride_id = r.ride_id
    left join dim_rider dr    on r.rider_id = dr.user_id
    left join dim_pmt dpmt    on p.payment_method_type_id = dpmt.payment_method_type_id
)

select * from final
