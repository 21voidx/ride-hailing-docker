-- Assert: no ride in fct_rides has a negative total_fare.
-- Tolerates NULL total_fare (rides without a FINAL fare record).
select
    ride_id,
    total_fare
from {{ ref('fct_rides') }}
where total_fare < 0
