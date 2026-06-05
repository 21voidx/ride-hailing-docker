{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ride_sk',
    partition_by={'field': 'requested_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['city_code', 'service_type', 'ride_status']
) }}

with base as (
    select * from {{ ref('int_ride_financials') }}
    {% if is_incremental() %}
    where requested_date >= date_sub((select coalesce(max(requested_date), date('2024-01-01')) from {{ this }}), interval 3 day)
    {% endif %}
)
select
    {{ dbt_utils.generate_surrogate_key(['b.ride_id']) }} as ride_sk,
    b.ride_id,
    dr.rider_sk,
    dd.driver_sk,
    dv.vehicle_sk,
    dpm.payment_method_sk,
    dp.promotion_sk,
    b.rider_id,
    b.driver_id,
    b.vehicle_id,
    b.payment_method_id,
    b.promotion_id,
    b.payment_transaction_id,
    b.fare_id,
    b.city_code,
    b.service_type,
    b.ride_status,
    b.lifecycle_outcome,
    b.requested_date,
    b.requested_at,
    b.accepted_at,
    b.arrived_at,
    b.started_at,
    b.completed_at,
    b.cancelled_at,
    b.cancelled_by_type,
    b.cancel_reason_code,
    b.is_completed,
    b.is_cancelled,
    b.is_payment_failed,
    b.is_weekend,
    b.is_peak_hour,
    b.requested_hour,
    b.estimated_distance_km,
    b.estimated_duration_min,
    b.distance_km,
    b.duration_min,
    b.accept_delay_min,
    b.driver_arrival_min,
    b.pickup_wait_min,
    b.actual_ride_duration_min,
    b.base_fare,
    b.distance_fare,
    b.time_fare,
    b.surge_multiplier,
    b.surge_amount,
    b.discount_amount,
    b.tax_amount,
    b.platform_fee,
    b.driver_earning,
    b.total_fare,
    b.payment_status,
    b.failure_code,
    b.payment_amount,
    b.payment_method_fee,
    b.promo_discount_amount,
    fb.review_id,
    fb.rating_score,
    fb.ticket_id,
    fb.ticket_category,
    fb.ticket_status,
    fb.priority,
    current_timestamp() as _dbt_loaded_at
from base b
left join {{ ref('dim_rider') }} dr
  on b.rider_id = dr.rider_id
 and b.requested_at >= dr.valid_from
 and b.requested_at < coalesce(dr.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_driver') }} dd
  on b.driver_id = dd.driver_id
 and coalesce(b.accepted_at, b.requested_at) >= dd.valid_from
 and coalesce(b.accepted_at, b.requested_at) < coalesce(dd.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_vehicle') }} dv
  on b.vehicle_id = dv.vehicle_id
 and coalesce(b.accepted_at, b.requested_at) >= dv.valid_from
 and coalesce(b.accepted_at, b.requested_at) < coalesce(dv.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_payment_method') }} dpm
  on b.payment_method_id = dpm.payment_method_id
 and coalesce(b.paid_at, b.completed_at, b.requested_at) >= dpm.valid_from
 and coalesce(b.paid_at, b.completed_at, b.requested_at) < coalesce(dpm.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_promotion') }} dp
  on b.promotion_id = dp.promotion_id
 and coalesce(b.promo_used_at, b.completed_at, b.requested_at) >= dp.valid_from
 and coalesce(b.promo_used_at, b.completed_at, b.requested_at) < coalesce(dp.valid_to, timestamp('9999-12-31'))
left join {{ ref('int_customer_feedback') }} fb using (ride_id)
