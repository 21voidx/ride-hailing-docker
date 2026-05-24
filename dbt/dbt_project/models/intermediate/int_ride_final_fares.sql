-- Intermediate: final fare terbaru per ride
-- Tujuan: mencegah fct_rides terduplikasi jika ada lebih dari satu FINAL fare version.
-- Grain: 1 row per ride_id

with fares as (
    select *
    from {{ ref('stg_ride_fare') }}
    where fare_type = 'FINAL'
),

ranked as (
    select
        *,
        row_number() over (
            partition by ride_id
            order by fare_version desc, calculated_at desc, fare_id desc
        ) as rn
    from fares
)

select
    fare_id,
    ride_id,
    fare_type,
    fare_version,
    currency_code,
    distance_km,
    duration_min,
    base_fare,
    distance_fare,
    time_fare,
    surge_multiplier,
    surge_amount,
    discount_amount,
    tax_amount,
    platform_fee,
    driver_earning,
    total_fare,
    fare_rule_code,
    calculated_at,
    created_at,
    is_surge,
    has_discount
from ranked
where rn = 1
