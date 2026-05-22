{{
    config(
        materialized='incremental',
        unique_key=['transaction_id', '__lsn'],
        on_schema_change='append_new_columns'
    )
}}

with events as (
    select *
    from {{ ref('stg_cdc__payment_transaction_events') }}
    where {{ cdc_partition_filter(days_back=3) }}
    {% if is_incremental() %}
    and __source_ts_ms > (
        select MAX(__source_ts_ms)
        from {{ this }}
    )
    {% endif %}
),

-- count events per transaction and detect retries/rollbacks
event_stats as (
    select
        transaction_id,
        COUNTIF(payment_status = 'AUTHORIZED')  as authorized_count,
        ARRAY_AGG(
            payment_status
            order by __lsn
        )                                       as status_sequence
    from events
    group by transaction_id
),

-- detect rollbacks: PAID → FAILED or REFUNDED → FAILED etc.
status_order as (
    select status, ord
    from (values
        ('AUTHORIZED', 1),
        ('CAPTURED',   2),
        ('PAID',       3),
        ('FAILED',     4),
        ('REFUNDED',   5)
    ) as t(status, ord)
),

events_with_lag as (
    select
        e.*,
        LAG(e.payment_status) OVER (
            PARTITION BY e.transaction_id ORDER BY e.__lsn
        ) as prev_payment_status
    from events e
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['ewl.transaction_id', 'ewl.__lsn']) }}
                                                                        as audit_key,
        ewl.transaction_id,
        ewl.ride_id,
        ewl.payment_status,
        ewl.amount,
        ewl.currency_code,
        ewl.failure_code,
        ewl.failure_message,
        ewl.authorized_at,
        ewl.captured_at,
        ewl.paid_at,
        ewl.created_at,
        ewl.__op,
        ewl.__lsn,
        ewl.__source_ts_ms,

        -- per-transaction aggregates
        COALESCE(es.authorized_count, 0)                                as authorized_event_count,
        (SELECT COUNT(*) FROM events e2 WHERE e2.transaction_id = ewl.transaction_id)
                                                                        as event_count,
        COALESCE(es.authorized_count > 1, false)                        as is_retry,

        -- rollback: current status has lower order than previous status
        COALESCE(
            (
                select so_curr.ord < so_prev.ord
                from status_order so_curr, status_order so_prev
                where so_curr.status = ewl.payment_status
                  and so_prev.status = ewl.prev_payment_status
            ),
            false
        )                                                               as is_status_rollback

    from events_with_lag ewl
    left join event_stats es on ewl.transaction_id = es.transaction_id
)

select * from final
