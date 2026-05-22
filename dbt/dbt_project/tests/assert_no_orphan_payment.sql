-- Assert: every payment in fct_payments is linked to a ride in fct_rides.
select
    p.transaction_id,
    p.ride_id
from {{ ref('fct_payments') }} p
left join {{ ref('fct_rides') }} r on p.ride_id = r.ride_id
where r.ride_id is null
