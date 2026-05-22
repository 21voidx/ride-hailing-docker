{{
    config(
        materialized='incremental',
        unique_key='ride_status_history_id',
        on_schema_change='append_new_columns'
    )
}}

with transitions as (
    select *
    from {{ ref('int__ride_status_transitions') }}
    {% if is_incremental() %}
    where changed_at > (
        select TIMESTAMP_SUB(MAX(changed_at), INTERVAL 1 HOUR)
        from {{ this }}
    )
    {% endif %}
),

rides as (
    select ride_id, rider_id, driver_id, city_code, service_type, ride_date
    from {{ ref('fct_rides') }}
),

final as (
    select
        t.ride_status_history_id,
        t.ride_id,
        r.rider_id,
        r.driver_id,
        r.city_code,
        r.service_type,
        t.old_status,
        t.new_status,
        t.changed_by_user_id,
        t.reason_code,
        t.reason_note,
        t.changed_at,
        t.changed_date,
        t.duration_seconds_in_prev_status,
        t.is_anomaly,
        t.event_max_lsn,
        t.event_max_source_ts_ms
    from transitions t
    left join rides r on t.ride_id = r.ride_id
)

select * from final
