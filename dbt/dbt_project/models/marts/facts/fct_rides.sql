{{ config(
    unique_key='ride_id',
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['city_code', 'service_type', 'ride_status', 'driver_id']
) }}

with rides as (
    select *
    from {{ ref('int_core__rides_unified') }}
    {% if is_incremental() %}
      where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
    {% endif %}
), lifecycle as (
    select * from {{ ref('int_core__ride_lifecycle') }}
), locations as (
    select * from {{ ref('int_core__ride_locations_pivoted') }}
)
select
    r.ride_id,
    {{ surrogate_key(["'ride'", 'r.ride_id']) }} as ride_key,
    r.rider_id,
    {{ surrogate_key(["'rider'", 'r.rider_id']) }} as rider_key,
    r.driver_id,
    {{ surrogate_key(["'driver'", 'r.driver_id']) }} as driver_key,
    r.vehicle_id,
    {{ surrogate_key(["'vehicle'", 'r.vehicle_id']) }} as vehicle_key,
    r.ride_status,
    r.service_type,
    r.city_code,
    r.requested_at,
    r.accepted_at,
    r.arrived_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.cancelled_by_user_id,
    r.cancel_reason_code,
    r.cancel_reason_note,
    r.estimated_distance_km,
    r.estimated_duration_min,
    r.requested_date,
    {{ surrogate_key(["'date'", 'r.requested_date']) }} as requested_date_key,
    r.requested_hour,
    r.completed_date,
    {{ surrogate_key(["'date'", 'r.completed_date']) }} as completed_date_key,
    r.is_completed,
    r.is_cancelled,
    r.is_payment_failed,
    l.request_to_accept_minutes,
    l.accept_to_arrive_minutes,
    l.pickup_wait_minutes,
    l.trip_minutes,
    l.request_to_complete_minutes,
    l.request_to_cancel_minutes,
    l.is_accept_sla_met,
    l.is_arrival_sla_met,
    loc.pickup_requested_latitude,
    loc.pickup_requested_longitude,
    loc.dropoff_requested_latitude,
    loc.dropoff_requested_longitude,
    loc.pickup_actual_latitude,
    loc.pickup_actual_longitude,
    loc.dropoff_actual_latitude,
    loc.dropoff_actual_longitude,
    r.created_at,
    r.updated_at,
    {{ audit_columns() }}
from rides r
left join lifecycle l on r.ride_id = l.ride_id
left join locations loc on r.ride_id = loc.ride_id
