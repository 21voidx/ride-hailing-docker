{{ config(
    unique_key='transaction_id',
    partition_by={'field': 'payment_created_date', 'data_type': 'date'},
    cluster_by=['ride_id', 'payment_status']
) }}

select
    p.transaction_id,
    p.ride_id,
    p.user_payment_method_id,
    upm.payment_method_type_id,
    p.provider_name,
    p.provider_transaction_id,
    p.idempotency_key,
    p.amount,
    p.method_fee,
    p.currency_code,
    p.payment_status,
    p.failure_code,
    p.failure_message,
    p.authorized_at,
    p.captured_at,
    p.paid_at,
    p.created_at,
    p.updated_at,
    date(p.created_at, '{{ var("timezone", "Asia/Jakarta") }}') as payment_created_date,
    p.is_paid,
    p.is_payment_failed,
    {{ audit_columns() }}
from {{ ref('int_core__payment_status_per_ride') }} p
left join {{ ref('stg_pg__user_payment_methods') }} upm
  on p.user_payment_method_id = upm.user_payment_method_id
{% if is_incremental() %}
where date(p.created_at, '{{ var("timezone", "Asia/Jakarta") }}') >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
{% endif %}
