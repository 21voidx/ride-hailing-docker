{{
    config(
        materialized='incremental',
        unique_key='ride_status_history_id',
        on_schema_change='append_new_columns'
    )
}}

with history as (
    select *
    from {{ ref('stg_pg__ride_status_history') }}
    {% if is_incremental() %}
    where changed_at > (
        select TIMESTAMP_SUB(MAX(changed_at), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

-- valid forward-only transitions
valid_transitions as (
    select
        old_status,
        new_status
    from (values
        (cast(null as STRING), 'REQUESTED'),
        ('REQUESTED',          'ACCEPTED'),
        ('REQUESTED',          'CANCELLED'),
        ('ACCEPTED',           'ARRIVED'),
        ('ACCEPTED',           'CANCELLED'),
        ('ARRIVED',            'IN_PROGRESS'),
        ('ARRIVED',            'CANCELLED'),
        ('IN_PROGRESS',        'COMPLETED'),
        ('IN_PROGRESS',        'PAYMENT_FAILED'),
        ('IN_PROGRESS',        'CANCELLED'),
        ('PAYMENT_FAILED',     'COMPLETED'),
        ('PAYMENT_FAILED',     'CANCELLED')
    ) as t(old_status, new_status)
),

with_lag as (
    select
        h.*,
        LAG(h.changed_at) OVER (
            PARTITION BY h.ride_id ORDER BY h.changed_at, h.ride_status_history_id
        )                                                               as prev_changed_at,
        LAG(h.new_status) OVER (
            PARTITION BY h.ride_id ORDER BY h.changed_at, h.ride_status_history_id
        )                                                               as prev_new_status
    from history h
),

-- enrich with CDC event tiebreaker
ride_events_latest as (
    select
        ride_id,
        MAX(__lsn)          as max_lsn,
        MAX(__source_ts_ms) as max_source_ts_ms
    from {{ ref('stg_cdc__ride_events') }}
    group by ride_id
),

final as (
    select
        wl.ride_status_history_id,
        wl.ride_id,
        wl.old_status,
        wl.new_status,
        wl.changed_by_user_id,
        wl.reason_code,
        wl.reason_note,
        wl.changed_at,
        wl.created_at,
        wl.changed_date,
        wl.prev_changed_at,
        wl.prev_new_status,

        -- duration in seconds spent in the previous status
        CASE
            WHEN wl.prev_changed_at IS NULL THEN NULL
            ELSE TIMESTAMP_DIFF(wl.changed_at, wl.prev_changed_at, SECOND)
        END                                                             as duration_seconds_in_prev_status,

        -- anomaly: negative duration or invalid transition
        (
            (wl.prev_changed_at IS NOT NULL
             and TIMESTAMP_DIFF(wl.changed_at, wl.prev_changed_at, SECOND) < 0)
            or not exists (
                select 1
                from valid_transitions vt
                where (vt.old_status = wl.old_status
                       or (vt.old_status is null and wl.old_status is null))
                  and vt.new_status = wl.new_status
            )
        )                                                               as is_anomaly,

        -- CDC tiebreaker from event log
        rel.max_lsn         as event_max_lsn,
        rel.max_source_ts_ms as event_max_source_ts_ms

    from with_lag wl
    left join ride_events_latest rel on wl.ride_id = rel.ride_id
)

select * from final
