with fare_latest as (
    select * except(rn)
    from (
        select f.*, row_number() over (partition by ride_id order by fare_version desc, updated_at desc) as rn
        from {{ ref('stg_ride_fare') }} f
    )
    where rn = 1
),
payment_latest as (
    select * except(rn)
    from (
        select p.*, row_number() over (partition by ride_id order by updated_at desc, payment_transaction_id desc) as rn
        from {{ ref('stg_payment_transaction') }} p
        where not is_deleted
    )
    where rn = 1
)
select
    r.ride_id,
    r.rider_id,
    r.driver_id,
    r.vehicle_id,
    r.city_code,
    r.service_type,
    r.ride_status,
    r.requested_date,
    r.requested_at,
    r.accepted_at,
    r.arrived_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.cancelled_by_type,
    r.cancel_reason_code,
    r.is_completed,
    r.is_cancelled,
    r.is_payment_failed,
    r.is_weekend,
    r.is_peak_hour,
    r.requested_hour,
    r.lifecycle_outcome,
    r.estimated_distance_km,
    r.estimated_duration_min,
    r.accept_delay_min,
    r.driver_arrival_min,
    r.pickup_wait_min,
    r.actual_ride_duration_min,
    f.fare_id,
    f.distance_km,
    f.duration_min,
    f.base_fare,
    f.distance_fare,
    f.time_fare,
    f.surge_multiplier,
    f.surge_amount,
    f.discount_amount,
    f.tax_amount,
    f.platform_fee,
    f.driver_earning,
    f.total_fare,
    p.payment_transaction_id,
    p.payment_method_id,
    p.payment_status,
    p.failure_code,
    p.amount as payment_amount,
    p.method_fee as payment_method_fee,
    p.paid_at,
    pu.promotion_id,
    pu.discount_amount_applied as promo_discount_amount,
    pu.used_at as promo_used_at
from {{ ref('int_ride_lifecycle') }} r
left join fare_latest f using (ride_id)
left join payment_latest p using (ride_id)
left join {{ ref('stg_promo_usage') }} pu using (ride_id)
