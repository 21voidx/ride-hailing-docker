{{ config(
    unique_key='ride_id',
    partition_by={'field': 'requested_date', 'data_type': 'date'},
    cluster_by=['city_code', 'service_type', 'ride_status', 'driver_id']
) }}

with rides as (
    select *
    from {{ ref('fct_rides') }}
    {% if is_incremental() %}
      where requested_date >= date_sub(current_date('{{ var("timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 3) }} day)
    {% endif %}
), fares as (
    select * from {{ ref('fct_ride_fares') }}
), payments as (
    select * from {{ ref('fct_payments') }}
), reviews as (
    select * from {{ ref('int_core__reviews_per_ride') }}
), promo as (
    select * from {{ ref('int_core__promo_usage_per_ride') }}
), drivers as (
    select * from {{ ref('dim_drivers') }}
), vehicles as (
    select * from {{ ref('dim_vehicles') }}
), payment_methods as (
    select * from {{ ref('dim_payment_methods') }}
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
    r.requested_hour,
    date_trunc(r.requested_date, week(monday)) as requested_week_start,
    date_trunc(r.requested_date, month) as requested_month_start,
    format_date('%A', r.requested_date) as requested_day_name,
    r.requested_at,
    r.accepted_at,
    r.arrived_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.is_completed,
    r.is_cancelled,
    r.is_payment_failed as is_ride_payment_failed,
    r.cancel_reason_code,
    r.cancel_reason_note,
    r.request_to_accept_minutes,
    r.accept_to_arrive_minutes,
    r.pickup_wait_minutes,
    r.trip_minutes,
    r.request_to_complete_minutes,
    r.request_to_cancel_minutes,
    r.is_accept_sla_met,
    r.is_arrival_sla_met,
    r.estimated_distance_km,
    r.estimated_duration_min,
    f.fare_id,
    f.currency_code,
    f.distance_km as final_distance_km,
    f.duration_min as final_duration_min,
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
    p.transaction_id,
    p.payment_status,
    p.payment_method_type_id,
    pm.method_code as payment_method_code,
    pm.method_name as payment_method_name,
    p.failure_code as payment_failure_code,
    p.amount as payment_amount,
    p.method_fee,
    p.is_paid,
    p.is_payment_failed as is_payment_transaction_failed,
    p.paid_at,
    promo.promotion_id,
    promo.promo_code,
    promo.discount_type as promo_discount_type,
    promo.discount_amount_applied,
    promo.promo_usage_id is not null as is_promo_used,
    coalesce(reviews.review_count, 0) as review_count,
    reviews.avg_rating_score,
    reviews.rider_to_driver_rating_score,
    reviews.rider_to_driver_comments,
    reviews.review_count is not null as has_review,
    d.driver_status as driver_status_latest,
    d.verification_status as driver_verification_status,
    d.rating_avg as driver_rating_avg,
    d.rating_count as driver_rating_count,
    v.vehicle_type,
    v.vehicle_make,
    v.vehicle_model,
    v.vehicle_year,
    v.vehicle_capacity,
    v.vehicle_status,
    r.pickup_requested_latitude,
    r.pickup_requested_longitude,
    r.dropoff_requested_latitude,
    r.dropoff_requested_longitude,
    r.pickup_actual_latitude,
    r.pickup_actual_longitude,
    r.dropoff_actual_latitude,
    r.dropoff_actual_longitude,
    {{ audit_columns() }}
from rides r
left join fares f on r.ride_id = f.ride_id
left join payments p on r.ride_id = p.ride_id
left join payment_methods pm on p.payment_method_type_id = pm.payment_method_type_id
left join reviews on r.ride_id = reviews.ride_id
left join promo on r.ride_id = promo.ride_id
left join drivers d on r.driver_id = d.driver_id
left join vehicles v on r.vehicle_id = v.vehicle_id
