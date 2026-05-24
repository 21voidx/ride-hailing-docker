-- Intermediate: ringkasan payment per ride
-- Tujuan: fct_rides tetap ber-grain 1 row per ride meskipun 1 ride punya banyak payment_transaction.
-- Grain: 1 row per ride_id

with payments as (
    select * from {{ ref('stg_payment_transaction') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by ride_id
            order by
                case when payment_status = 'PAID' then 1 else 0 end desc,
                coalesce(paid_at, captured_at, authorized_at, updated_at, created_at) desc,
                transaction_id desc
        ) as selected_payment_rank
    from payments
),

aggregated as (
    select
        ride_id,
        count(*) as payment_transaction_count,
        countif(payment_status = 'PAID') as paid_payment_count,
        countif(payment_status = 'FAILED') as failed_payment_count,
        countif(payment_status = 'REFUNDED') as refunded_payment_count,
        sum(amount) as gross_payment_amount,
        sum(method_fee) as gross_method_fee,
        sum(total_charged) as gross_total_charged,
        max(updated_at) as latest_payment_updated_at
    from payments
    group by ride_id
),

selected as (
    select
        ride_id,
        transaction_id,
        user_payment_method_id,
        provider_name,
        provider_transaction_id,
        payment_status,
        failure_code,
        failure_message,
        authorized_at,
        captured_at,
        paid_at,
        created_at,
        updated_at,
        amount,
        method_fee,
        total_charged,
        is_paid,
        is_failed,
        is_refunded
    from ranked
    where selected_payment_rank = 1
)

select
    a.ride_id,
    s.transaction_id as selected_transaction_id,
    s.user_payment_method_id,
    s.provider_name,
    s.provider_transaction_id,
    s.payment_status,
    s.failure_code,
    s.failure_message,
    s.authorized_at,
    s.captured_at,
    s.paid_at,
    s.created_at as selected_payment_created_at,
    s.updated_at as selected_payment_updated_at,
    s.amount as selected_payment_amount,
    s.method_fee as selected_payment_method_fee,
    s.total_charged as selected_total_charged,
    s.is_paid,
    s.is_failed,
    s.is_refunded,
    a.payment_transaction_count,
    a.paid_payment_count,
    a.failed_payment_count,
    a.refunded_payment_count,
    a.gross_payment_amount,
    a.gross_method_fee,
    a.gross_total_charged,
    a.latest_payment_updated_at
from aggregated a
left join selected s on a.ride_id = s.ride_id
