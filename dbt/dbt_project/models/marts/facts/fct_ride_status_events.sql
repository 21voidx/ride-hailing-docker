-- Fact: Ride Status Events
-- Grain    : 1 row per ride status event
-- Source   : Batch ride_status_history + current ride state
-- Strategy : incremental merge by ride_status_event_sk

{{
    config(
        materialized='incremental',
        unique_key='ride_status_event_sk',
        incremental_strategy='merge',
        partition_by={
            'field': 'status_event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['ride_id', 'new_status'],
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

changed_ride_ids as (
    select distinct ride_id
    from {{ ref('stg_ride_status_history') }}
    where created_at >= (select watermark from source_watermark)
),

events as (
    select e.*
    from {{ ref('stg_ride_status_history') }} e
    {% if is_incremental() %}
        inner join changed_ride_ids c on e.ride_id = c.ride_id
    {% endif %}
),

sequenced as (
    select
        e.*,
        row_number() over (
            partition by ride_id
            order by changed_at, ride_status_history_id
        ) as status_sequence,
        lead(changed_at) over (
            partition by ride_id
            order by changed_at, ride_status_history_id
        ) as next_changed_at
    from events e
),

rides as (
    select
        ride_id,
        rider_id,
        driver_id,
        vehicle_id,
        service_type,
        city_code,
        requested_at,
        completed_at,
        cancelled_at,
        updated_at as ride_updated_at
    from {{ ref('stg_ride') }}
),

dim_user as (
    select user_id, user_sk from {{ ref('dim_user') }}
),

dim_driver as (
    select driver_id, driver_sk from {{ ref('dim_driver') }}
),

dim_vehicle as (
    select vehicle_id, vehicle_sk from {{ ref('dim_vehicle') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.ride_status_history_id']) }} as ride_status_event_sk,
        s.ride_status_history_id,
        s.ride_id,

        du.user_sk as rider_sk,
        dcu.user_sk as changed_by_user_sk,
        dd.driver_sk,
        dv.vehicle_sk,

        cast(format_date('%Y%m%d', date(s.changed_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as int64) as status_event_date_key,

        r.service_type,
        r.city_code,
        s.old_status,
        s.new_status,
        s.reason_code,
        s.reason_note,
        s.status_sequence,
        s.changed_by_user_id,

        s.new_status in ('COMPLETED', 'CANCELLED', 'PAYMENT_FAILED') as is_terminal_status,
        s.old_status is null as is_initial_status,

        s.changed_at,
        s.next_changed_at,
        date(s.changed_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as status_event_date,
        timestamp_trunc(s.changed_at, hour, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as status_event_hour,

        1 as status_event_count,
        timestamp_diff(s.next_changed_at, s.changed_at, second) as seconds_until_next_status,
        timestamp_diff(s.changed_at, r.requested_at, second) as seconds_since_request,

        s.created_at as status_event_created_at,
        greatest(
            coalesce(s.created_at, timestamp('1900-01-01')),
            coalesce(r.ride_updated_at, timestamp('1900-01-01'))
        ) as _last_source_updated_at,
        current_timestamp() as _dbt_loaded_at

    from sequenced s
    left join rides r on s.ride_id = r.ride_id
    left join dim_user du on r.rider_id = du.user_id
    left join dim_user dcu on s.changed_by_user_id = dcu.user_id
    left join dim_driver dd on r.driver_id = dd.driver_id
    left join dim_vehicle dv on r.vehicle_id = dv.vehicle_id
)

select * from final
