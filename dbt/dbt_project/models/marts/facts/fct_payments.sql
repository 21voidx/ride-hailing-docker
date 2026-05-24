-- Fact: Payments
-- Grain    : 1 row per payment_transaction
-- Keys     : payment_sk (SK, PK), transaction_id (NK), + surrogate FKs ke dimensi
-- Source   : CDC payment_transaction + Batch payment_refund
-- Strategy : incremental merge by payment_sk

{{
    config(
        materialized='incremental',
        unique_key='payment_sk',
        incremental_strategy='merge',
        partition_by={
            'field': 'transaction_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['payment_status', 'method_code'],
        on_schema_change='sync_all_columns'
    )
}}

with source_watermark as (
    {% if is_incremental() %}
        select timestamp_sub(
            coalesce(max(_last_source_updated_at), timestamp('1900-01-01')),
            interval {{ var('incremental_lookback_days', 2) }} day
        ) as watermark
        from {{ this }}
    {% else %}
        select timestamp('1900-01-01') as watermark
    {% endif %}
),

changed_transaction_ids as (
    select transaction_id
    from {{ ref('stg_payment_transaction') }}
    where updated_at >= (select watermark from source_watermark)

    union distinct
    select transaction_id
    from {{ ref('stg_payment_refund') }}
    where updated_at >= (select watermark from source_watermark)
),

payments as (
    select p.*
    from {{ ref('stg_payment_transaction') }} p
    {% if is_incremental() %}
        inner join changed_transaction_ids c on p.transaction_id = c.transaction_id
    {% endif %}
),

refunds as (
    select
        transaction_id,
        sum(refund_amount) as total_refunded,
        array_agg(refund_status order by coalesce(completed_at, requested_at) desc limit 1)[offset(0)] as latest_refund_status,
        min(requested_at) as first_refund_requested_at,
        max(completed_at) as last_refund_completed_at,
        count(*) as refund_count,
        max(updated_at) as latest_refund_updated_at
    from {{ ref('stg_payment_refund') }}
    group by transaction_id
),

payment_methods as (
    select * from {{ ref('stg_user_payment_method') }}
),

payment_method_types as (
    select * from {{ ref('stg_payment_method_type') }}
),

rides as (
    select ride_id, rider_id, driver_id, service_type, city_code
    from {{ ref('stg_ride') }}
),

dim_user as (
    select user_id, user_sk from {{ ref('dim_user') }}
),

dim_payment_method as (
    select user_payment_method_id, payment_method_sk from {{ ref('dim_payment_method') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.transaction_id']) }} as payment_sk,
        p.transaction_id,

        du.user_sk as rider_sk,
        dpm.payment_method_sk,

        cast(format_date('%Y%m%d', date(p.created_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as int64) as transaction_date_key,

        p.ride_id,
        p.provider_name,
        p.provider_transaction_id,
        p.payment_status,
        p.failure_code,
        p.failure_message,
        pmt.method_code,
        pmt.method_name,
        r.service_type,
        r.city_code,

        p.is_paid,
        p.is_failed,
        p.is_refunded,
        coalesce(r_ref.refund_count, 0) > 0 as has_refund,

        p.authorized_at,
        p.captured_at,
        p.paid_at,
        p.created_at as transaction_created_at,
        p.updated_at as transaction_updated_at,
        date(p.created_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as transaction_date,
        timestamp_trunc(p.created_at, hour, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as transaction_hour,

        p.amount,
        p.method_fee,
        p.total_charged,
        coalesce(r_ref.total_refunded, 0) as total_refunded,
        p.total_charged - coalesce(r_ref.total_refunded, 0) as net_revenue,

        timestamp_diff(p.captured_at, p.authorized_at, second) as auth_to_capture_sec,
        timestamp_diff(p.paid_at, p.captured_at, second) as capture_to_paid_sec,

        coalesce(r_ref.refund_count, 0) as refund_count,
        r_ref.latest_refund_status,
        r_ref.first_refund_requested_at,
        r_ref.last_refund_completed_at,

        greatest(
            coalesce(p.updated_at, timestamp('1900-01-01')),
            coalesce(r_ref.latest_refund_updated_at, timestamp('1900-01-01'))
        ) as _last_source_updated_at,
        current_timestamp() as _dbt_loaded_at

    from payments p
    left join rides r on p.ride_id = r.ride_id
    left join payment_methods pm on p.user_payment_method_id = pm.user_payment_method_id
    left join payment_method_types pmt on pm.payment_method_type_id = pmt.payment_method_type_id
    left join dim_user du on r.rider_id = du.user_id
    left join dim_payment_method dpm on p.user_payment_method_id = dpm.user_payment_method_id
    left join refunds r_ref on p.transaction_id = r_ref.transaction_id
)

select * from final
