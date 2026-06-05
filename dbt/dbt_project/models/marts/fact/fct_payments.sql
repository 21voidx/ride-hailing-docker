{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_transaction_sk',
    partition_by={'field': 'payment_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['payment_status', 'provider_name']
) }}

with base as (
    select *, date(created_at, 'Asia/Jakarta') as payment_date
    from {{ ref('stg_payment_transaction') }}
    where not is_deleted
    {% if is_incremental() %}
      and date(updated_at, 'Asia/Jakarta') >= date_sub((select coalesce(max(payment_date), date('2024-01-01')) from {{ this }}), interval 3 day)
    {% endif %}
)
select
    {{ dbt_utils.generate_surrogate_key(['b.payment_transaction_id']) }} as payment_transaction_sk,
    b.payment_transaction_id,
    fr.ride_sk,
    dr.rider_sk,
    dpm.payment_method_sk,
    b.ride_id,
    b.rider_id,
    b.payment_method_id,
    b.provider_name,
    b.provider_transaction_id,
    b.idempotency_key,
    b.amount,
    b.method_fee,
    b.currency_code,
    b.payment_status,
    b.failure_code,
    b.failure_message,
    b.authorized_at,
    b.captured_at,
    b.paid_at,
    b.created_at,
    b.updated_at,
    b.payment_date,
    current_timestamp() as _dbt_loaded_at
from base b
left join {{ ref('fct_rides') }} fr using (ride_id)
left join {{ ref('dim_rider') }} dr
  on b.rider_id = dr.rider_id
 and b.created_at >= dr.valid_from
 and b.created_at < coalesce(dr.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_payment_method') }} dpm
  on b.payment_method_id = dpm.payment_method_id
 and b.created_at >= dpm.valid_from
 and b.created_at < coalesce(dpm.valid_to, timestamp('9999-12-31'))
