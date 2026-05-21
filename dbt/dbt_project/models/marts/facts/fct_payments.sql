{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'txn_date',
            'data_type': 'date'
        },
        cluster_by=['payment_status', 'provider_name']
    )
}}

with payment_enriched as (

    select * from {{ ref('int__payment_enriched') }}

    {% if is_incremental() %}
    where __source_ts_ms > (
        select coalesce(MAX(__source_ts_ms), 0)
        from {{ this }}
    )
    {% endif %}

),

dim_rider as (
    select rider_id from {{ ref('dim_rider') }}
),

dim_payment_method_type as (
    select payment_method_type_id, method_code, method_name
    from {{ ref('dim_payment_method_type') }}
),

final as (

    select
        p.transaction_id,
        p.ride_id,
        p.user_payment_method_id,
        p.user_id                                    as rider_id,
        p.payment_method_type_id,

        p.txn_date,
        p.provider_name,
        p.provider_transaction_id,
        p.idempotency_key,

        p.amount,
        p.method_fee,
        p.currency_code,
        p.net_amount,
        p.total_refunded,
        p.refund_count,

        p.payment_status,
        p.failure_code,
        p.failure_message,
        p.is_paid,
        p.is_failed,
        p.is_refunded,

        p.authorized_at,
        p.captured_at,
        p.paid_at,
        p.created_at,
        p.updated_at,

        p.masked_account,
        p.is_default_payment_method,
        p.payment_method_status,
        p.method_code,
        p.method_name,

        p.__source_ts_ms,
        CURRENT_TIMESTAMP()                          as _dbt_loaded_at

    from payment_enriched p
    left join dim_rider dr on p.user_id = dr.rider_id

)

select * from final
