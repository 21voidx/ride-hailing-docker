-- Singular test: assert that the sum of fare components per fare_id
-- is approximately equal to the total_fare in fct_rides.
-- Tolerance of 1 IDR is allowed for floating-point rounding differences.
-- A non-empty result indicates a fare reconciliation discrepancy.

with component_totals as (

    select
        fc.fare_id,
        SUM(fc.component_amount) as sum_of_components

    from {{ ref('stg_pg__ride_fare_components') }} fc
    group by 1

),

ride_fares as (

    select
        fare_id,
        ride_id,
        total_fare

    from {{ ref('stg_pg__ride_fares') }}

),

comparison as (

    select
        rf.fare_id,
        rf.ride_id,
        rf.total_fare,
        ct.sum_of_components,
        ABS(rf.total_fare - ct.sum_of_components) as discrepancy

    from ride_fares rf
    inner join component_totals ct on rf.fare_id = ct.fare_id

)

select
    fare_id,
    ride_id,
    total_fare,
    sum_of_components,
    discrepancy
from comparison
where discrepancy > 1
