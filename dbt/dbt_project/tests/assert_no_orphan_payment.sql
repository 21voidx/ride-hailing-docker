-- Singular test: assert no payment transaction references a ride_id
-- that does not exist in fct_rides.
-- A non-empty result indicates orphaned payment records.

select
    p.transaction_id,
    p.ride_id
from {{ ref('fct_payments') }} p
left join {{ ref('fct_rides') }} r
    using (ride_id)
where r.ride_id is null
