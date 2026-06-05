select
    r.*,
    timestamp_diff(accepted_at, requested_at, minute) as accept_delay_min,
    timestamp_diff(arrived_at, accepted_at, minute) as driver_arrival_min,
    timestamp_diff(started_at, arrived_at, minute) as pickup_wait_min,
    timestamp_diff(completed_at, started_at, minute) as actual_ride_duration_min,
    case
      when is_completed then 'completed'
      when is_cancelled then 'cancelled'
      when is_payment_failed then 'payment_failed'
      else 'incomplete'
    end as lifecycle_outcome
from {{ ref('stg_ride') }} r
where not is_deleted
