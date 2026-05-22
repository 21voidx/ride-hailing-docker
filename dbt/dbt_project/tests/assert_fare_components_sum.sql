-- Assert: sum of fare components per fare_id is within 1 IDR of the total_fare
-- recorded in fct_rides for FINAL fares (rounding tolerance).
with component_totals as (
    select
        fare_id,
        SUM(component_amount) as component_sum
    from {{ ref('stg_pg__ride_fare_components') }}
    group by fare_id
),

final_fares as (
    select
        fare_id,
        ride_id,
        total_fare
    from {{ ref('stg_pg__ride_fares') }}
    where fare_type = 'FINAL'
)

select
    ff.ride_id,
    ff.fare_id,
    ff.total_fare,
    ct.component_sum,
    ABS(ff.total_fare - ct.component_sum) as diff
from final_fares ff
join component_totals ct on ff.fare_id = ct.fare_id
where ABS(ff.total_fare - ct.component_sum) > 1
