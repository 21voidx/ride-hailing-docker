{{
    config(
        materialized='incremental',
        unique_key='ride_status_history_id',
        on_schema_change='sync_all_columns'
    )
}}

with status_history as (

    select * from {{ ref('stg_pg__ride_status_history') }}

    {% if is_incremental() %}
    where updated_at > (
        select coalesce(MAX(changed_at), '2020-01-01')
        from {{ this }}
    )
    {% endif %}

),

valid_transitions as (

    select
        old_status,
        new_status
    from UNNEST([
        STRUCT('REQUESTED'     as old_status, 'ACCEPTED'       as new_status),
        STRUCT('REQUESTED'     as old_status, 'CANCELLED'      as new_status),
        STRUCT('ACCEPTED'      as old_status, 'DRIVER_ARRIVED' as new_status),
        STRUCT('ACCEPTED'      as old_status, 'CANCELLED'      as new_status),
        STRUCT('DRIVER_ARRIVED'as old_status, 'STARTED'        as new_status),
        STRUCT('DRIVER_ARRIVED'as old_status, 'CANCELLED'      as new_status),
        STRUCT('STARTED'       as old_status, 'COMPLETED'      as new_status),
        STRUCT('STARTED'       as old_status, 'CANCELLED'      as new_status)
    ])

),

with_lag as (

    select
        sh.ride_status_history_id,
        sh.ride_id,
        sh.old_status,
        sh.new_status,
        sh.changed_by_user_id,
        sh.reason_code,
        sh.reason_note,
        sh.changed_at,
        sh.changed_date,
        sh.created_at,

        LAG(sh.changed_at) OVER (
            PARTITION BY sh.ride_id
            ORDER BY sh.changed_at ASC, sh.ride_status_history_id ASC
        ) as prev_status_changed_at,

        TIMESTAMP_DIFF(
            sh.changed_at,
            LAG(sh.changed_at) OVER (
                PARTITION BY sh.ride_id
                ORDER BY sh.changed_at ASC, sh.ride_status_history_id ASC
            ),
            SECOND
        ) as duration_seconds_in_prev_status

    from status_history sh

),

flagged as (

    select
        wl.*,
        (
            wl.duration_seconds_in_prev_status is not null
            and wl.duration_seconds_in_prev_status < 0
        ) or (
            wl.old_status is not null
            and not exists (
                select 1 from valid_transitions vt
                where vt.old_status = wl.old_status
                  and vt.new_status = wl.new_status
            )
        ) as is_anomaly

    from with_lag wl

)

select * from flagged
