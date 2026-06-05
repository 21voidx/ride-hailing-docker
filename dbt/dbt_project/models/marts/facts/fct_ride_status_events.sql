-- Fact: Ride Status Events
-- Grain    : 1 row per ride status event
-- Source   : CDC ride_events via stg_ride_events
-- Strategy : incremental merge by ride_status_event_sk
-- Notes    : Jangan mengambil dari stg_ride, karena stg_ride adalah current-state
--            1 row per ride setelah dedup CDC. Status event harus memakai event stream.

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
    from {{ ref('stg_ride_events') }}
    where greatest(
        coalesce(updated_at, timestamp('1900-01-01')),
        coalesce(_cdc_source_ts, timestamp('1900-01-01'))
    ) >= (select watermark from source_watermark)
),

events as (
    select e.*
    from {{ ref('stg_ride_events') }} e
    {% if is_incremental() %}
        inner join changed_ride_ids c on e.ride_id = c.ride_id
    {% endif %}
),

sequenced as (
    select
        e.*,
        row_number() over (
            partition by ride_id
            order by changed_at, _cdc_source_ts, _cdc_lsn
        ) as status_sequence,
        lead(changed_at) over (
            partition by ride_id
            order by changed_at, _cdc_source_ts, _cdc_lsn
        ) as next_changed_at
    from events e
),

dim_user as (
    select user_id, user_sk
    from {{ ref('dim_user') }}
),

dim_driver as (
    select
        driver_id,
        driver_sk,
        user_id as driver_user_id
    from {{ ref('dim_driver') }}
),

dim_vehicle as (
    select vehicle_id, vehicle_sk
    from {{ ref('dim_vehicle') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.ride_status_event_id']) }} as ride_status_event_sk,
        s.ride_status_event_id,
        s.ride_cdc_event_sk,
        s.ride_id,

        du.user_sk as rider_sk,
        dcu.user_sk as changed_by_user_sk,
        dd.driver_sk,
        dv.vehicle_sk,

        cast(format_date('%Y%m%d', date(s.changed_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as int64) as status_event_date_key,

        s.service_type,
        s.city_code,
        s.old_status,
        s.new_status,
        s.reason_code,
        s.reason_note,
        s.status_sequence,

        case
            when s.new_status = 'REQUESTED' then s.rider_id
            when s.new_status = 'CANCELLED' then s.cancelled_by_user_id
            when s.new_status in ('ACCEPTED', 'ARRIVED', 'IN_PROGRESS', 'COMPLETED', 'PAYMENT_FAILED') then dd.driver_user_id
            else null
        end as changed_by_user_id,

        s.new_status in ('COMPLETED', 'CANCELLED', 'PAYMENT_FAILED') as is_terminal_status,
        s.old_status is null as is_initial_status,

        s.changed_at,
        s.next_changed_at,
        date(s.changed_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as status_event_date,
        timestamp_trunc(s.changed_at, hour, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as status_event_hour,

        1 as status_event_count,
        timestamp_diff(s.next_changed_at, s.changed_at, second) as seconds_until_next_status,
        timestamp_diff(s.changed_at, s.requested_at, second) as seconds_since_request,

        s._cdc_op,
        s._cdc_lsn,
        s._cdc_source_ts,
        s.created_at as status_event_created_at,
        s.updated_at as status_event_updated_at,

        greatest(
            coalesce(s.updated_at, timestamp('1900-01-01')),
            coalesce(s._cdc_source_ts, timestamp('1900-01-01'))
        ) as _last_source_updated_at,
        current_timestamp() as _dbt_loaded_at

    from sequenced s
    left join dim_user du on s.rider_id = du.user_id
    left join dim_driver dd on s.driver_id = dd.driver_id
    left join dim_vehicle dv on s.vehicle_id = dv.vehicle_id
    left join dim_user dcu on (
        case
            when s.new_status = 'REQUESTED' then s.rider_id
            when s.new_status = 'CANCELLED' then s.cancelled_by_user_id
            when s.new_status in ('ACCEPTED', 'ARRIVED', 'IN_PROGRESS', 'COMPLETED', 'PAYMENT_FAILED') then dd.driver_user_id
            else null
        end
    ) = dcu.user_id
)

select * from final
