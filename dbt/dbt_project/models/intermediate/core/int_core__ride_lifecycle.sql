{{ config(materialized='view') }}

select
    ride_id,
    requested_at,
    accepted_at,
    arrived_at,
    started_at,
    completed_at,
    cancelled_at,
    {{ minutes_between('requested_at', 'accepted_at') }} as request_to_accept_minutes,
    {{ minutes_between('accepted_at', 'arrived_at') }} as accept_to_arrive_minutes,
    {{ minutes_between('arrived_at', 'started_at') }} as pickup_wait_minutes,
    {{ minutes_between('started_at', 'completed_at') }} as trip_minutes,
    {{ minutes_between('requested_at', 'completed_at') }} as request_to_complete_minutes,
    {{ minutes_between('requested_at', 'cancelled_at') }} as request_to_cancel_minutes,
    coalesce({{ minutes_between('requested_at', 'accepted_at') }} <= 5, false) as is_accept_sla_met,
    coalesce({{ minutes_between('accepted_at', 'arrived_at') }} <= 15, false) as is_arrival_sla_met,
    {{ audit_columns() }}
from {{ ref('int_core__rides_unified') }}
