{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='refund_sk',
    partition_by={'field': 'refund_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['refund_status', 'refund_reason_code']
) }}

with base as (
    select *, date(requested_at, 'Asia/Jakarta') as refund_date
    from {{ ref('stg_payment_refund') }}
    {% if is_incremental() %}
      where date(updated_at, 'Asia/Jakarta') >= date_sub((select coalesce(max(refund_date), date('2024-01-01')) from {{ this }}), interval 3 day)
    {% endif %}
),

payments as (
    select payment_transaction_id, payment_transaction_sk
    from {{ ref('fct_payments') }}
),

rides as (
    select ride_id, ride_sk
    from {{ ref('fct_rides') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['b.refund_id']) }} as refund_sk,
    b.refund_id,
    fp.payment_transaction_sk,
    fr.ride_sk,
    b.payment_transaction_id,
    b.ride_id,
    b.refund_amount,
    b.refund_reason_code,
    b.refund_status,
    b.requested_at,
    b.processed_at,
    b.created_at,
    b.updated_at,
    b.refund_date,
    timestamp_diff(b.processed_at, b.requested_at, minute) as refund_processing_min,
    current_timestamp() as _dbt_loaded_at
from base b
left join payments fp
    on b.payment_transaction_id = fp.payment_transaction_id
left join rides fr
    on b.ride_id = fr.ride_id
