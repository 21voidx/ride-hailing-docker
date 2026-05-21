-- Singular test: assert no ride has a negative total_fare.
-- A non-empty result indicates a test failure.

select
    ride_id,
    total_fare
from {{ ref('fct_rides') }}
where total_fare < 0
