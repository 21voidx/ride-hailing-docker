-- Fact: Rides
-- Grain    : 1 row per ride
-- Keys     : ride_sk (SK, PK), ride_id (NK), + surrogate FKs ke dimensi
-- Source   : CDC ride + intermediate ringkasan fare/payment/review/location
-- Strategy : incremental merge by ride_sk

{{
    config(
        materialized='incremental',
        unique_key='ride_sk',
        incremental_strategy='merge',
        partition_by={
            'field': 'ride_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['city_code', 'service_type', 'ride_status'],
        on_schema_change='sync_all_columns'
    )
}}

with source_watermark as (
    {% if is_incremental() %}
        select timestamp_sub(
            coalesce(max(_last_source_updated_at), timestamp('1900-01-01')),
            interval {{ var('incremental_lookback_days', 2) }} day
        ) as watermark
        from {{ this }}
    {% else %}
        select timestamp('1900-01-01') as watermark
    {% endif %}
),

changed_ride_ids as (
    select ride_id
    from {{ ref('stg_ride') }}
    where updated_at >= (select watermark from source_watermark)

    union distinct
    select ride_id
    from {{ ref('stg_ride_fare') }}
    where created_at >= (select watermark from source_watermark)

    union distinct
    select ride_id
    from {{ ref('stg_payment_transaction') }}
    where updated_at >= (select watermark from source_watermark)

    union distinct
    select ride_id
    from {{ ref('stg_promo_usage') }}
    where created_at >= (select watermark from source_watermark)

    union distinct
    select ride_id
    from {{ ref('stg_review') }}
    where updated_at >= (select watermark from source_watermark)

    union distinct
    select ride_id
    from {{ ref('stg_ride_location') }}
    where created_at >= (select watermark from source_watermark)
),

rides as (
    select r.*
    from {{ ref('stg_ride') }} r
    {% if is_incremental() %}
        inner join changed_ride_ids c on r.ride_id = c.ride_id
    {% endif %}
),

fares as (
    select * from {{ ref('int_ride_final_fares') }}
),

payment_summary as (
    select * from {{ ref('int_ride_payment_summary') }}
),

reviews as (
    select * from {{ ref('int_ride_reviews_pivoted') }}
),

promo_usages as (
    select * from {{ ref('stg_promo_usage') }}
),

promos as (
    select * from {{ ref('stg_promotion') }}
),

locations as (
    select * from {{ ref('int_ride_locations_pivoted') }}
),

dim_driver as (
    select driver_id, driver_sk from {{ ref('dim_driver') }}
),

dim_user as (
    select user_id, user_sk from {{ ref('dim_user') }}
),

dim_vehicle as (
    select vehicle_id, vehicle_sk from {{ ref('dim_vehicle') }}
),

dim_payment_method as (
    select user_payment_method_id, payment_method_sk from {{ ref('dim_payment_method') }}
),

dim_promotion as (
    select promotion_id, promotion_sk from {{ ref('dim_promotion') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['r.ride_id']) }} as ride_sk,
        r.ride_id,

        dd.driver_sk,
        du.user_sk as rider_sk,
        dv.vehicle_sk,
        dpm.payment_method_sk,
        dpr.promotion_sk,

        cast(format_date('%Y%m%d', date(r.requested_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as int64) as ride_date_key,
        cast(format_date('%Y%m%d', date(r.completed_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as int64) as complete_date_key,

        r.ride_id as ride_number,
        r.service_type,
        r.city_code,
        r.ride_status,
        r.cancel_reason_code,
        r.cancel_reason_note,
        p.payment_status,
        p.provider_name as payment_provider,
        p.provider_transaction_id,
        p.failure_code as payment_failure_code,
        p.failure_message as payment_failure_message,

        pu.promo_usage_id is not null as is_promo_used,
        prm.promo_code,
        prm.discount_type as promo_discount_type,

        r.ride_status = 'COMPLETED' as is_completed,
        r.ride_status = 'CANCELLED' as is_cancelled,
        r.ride_status = 'PAYMENT_FAILED' as is_payment_failed,
        coalesce(f.is_surge, false) as is_surge,
        coalesce(f.has_discount, false) as has_discount,
        coalesce(p.is_paid, false) as is_paid,
        coalesce(p.is_failed, false) as is_payment_transaction_failed,
        coalesce(p.is_refunded, false) as is_refunded,
        coalesce(rv.has_rider_review, false) as has_rider_review,
        coalesce(rv.has_driver_review, false) as has_driver_review,

        r.requested_at,
        r.accepted_at,
        r.arrived_at,
        r.started_at,
        r.completed_at,
        r.cancelled_at,
        p.authorized_at as payment_authorized_at,
        p.captured_at as payment_captured_at,
        p.paid_at,

        date(r.requested_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as ride_date,
        timestamp_trunc(r.requested_at, hour, '{{ var("reporting_timezone", "Asia/Jakarta") }}') as ride_hour,
        extract(hour from datetime(r.requested_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as request_hour,
        extract(dayofweek from date(r.requested_at, '{{ var("reporting_timezone", "Asia/Jakarta") }}')) as request_dow,

        timestamp_diff(r.accepted_at,  r.requested_at, second) as wait_to_accept_sec,
        timestamp_diff(r.arrived_at,   r.accepted_at,  second) as pickup_travel_sec,
        timestamp_diff(r.started_at,   r.arrived_at,   second) as pickup_wait_sec,
        timestamp_diff(r.completed_at, r.started_at,   second) as ride_duration_sec,
        timestamp_diff(r.completed_at, r.requested_at, second) as total_elapsed_sec,

        r.estimated_distance_km,
        r.estimated_duration_min,

        coalesce(f.distance_km, 0) as actual_distance_km,
        coalesce(f.duration_min, 0) as actual_duration_min,
        coalesce(f.base_fare, 0) as base_fare,
        coalesce(f.distance_fare, 0) as distance_fare,
        coalesce(f.time_fare, 0) as time_fare,
        coalesce(f.surge_multiplier, 1) as surge_multiplier,
        coalesce(f.surge_amount, 0) as surge_amount,
        coalesce(f.discount_amount, 0) as fare_discount_amount,
        coalesce(f.tax_amount, 0) as tax_amount,
        coalesce(f.platform_fee, 0) as platform_fee,
        coalesce(f.driver_earning, 0) as driver_earning,
        coalesce(f.total_fare, 0) as total_fare,

        coalesce(p.selected_payment_amount, 0) as selected_payment_amount,
        coalesce(p.selected_payment_method_fee, 0) as selected_payment_method_fee,
        coalesce(p.selected_total_charged, 0) as selected_total_charged,
        coalesce(p.payment_transaction_count, 0) as payment_transaction_count,
        coalesce(p.paid_payment_count, 0) as paid_payment_count,
        coalesce(p.failed_payment_count, 0) as failed_payment_count,
        coalesce(p.refunded_payment_count, 0) as refunded_payment_count,
        coalesce(p.gross_payment_amount, 0) as gross_payment_amount,
        coalesce(p.gross_method_fee, 0) as gross_method_fee,
        coalesce(p.gross_total_charged, 0) as gross_total_charged,

        coalesce(pu.discount_amount_applied, 0) as promo_discount_applied,

        rv.rider_rating_given,
        rv.driver_rating_given,

        loc.pickup_req_lat,
        loc.pickup_req_lng,
        loc.pickup_req_address,
        loc.dropoff_req_lat,
        loc.dropoff_req_lng,
        loc.dropoff_req_address,
        loc.pickup_actual_lat,
        loc.pickup_actual_lng,
        loc.dropoff_actual_lat,
        loc.dropoff_actual_lng,

        r.created_at as ride_created_at,
        r.updated_at as ride_updated_at,
        greatest(
            coalesce(r.updated_at, timestamp('1900-01-01')),
            coalesce(f.created_at, timestamp('1900-01-01')),
            coalesce(p.latest_payment_updated_at, timestamp('1900-01-01')),
            coalesce(pu.created_at, timestamp('1900-01-01')),
            coalesce(rv.latest_review_updated_at, timestamp('1900-01-01'))
        ) as _last_source_updated_at,
        current_timestamp() as _dbt_loaded_at

    from rides r
    left join dim_driver dd on r.driver_id = dd.driver_id
    left join dim_user du on r.rider_id = du.user_id
    left join dim_vehicle dv on r.vehicle_id = dv.vehicle_id
    left join fares f on r.ride_id = f.ride_id
    left join payment_summary p on r.ride_id = p.ride_id
    left join dim_payment_method dpm on p.user_payment_method_id = dpm.user_payment_method_id
    left join promo_usages pu on r.ride_id = pu.ride_id
    left join promos prm on pu.promotion_id = prm.promotion_id
    left join dim_promotion dpr on pu.promotion_id = dpr.promotion_id
    left join reviews rv on r.ride_id = rv.ride_id
    left join locations loc on r.ride_id = loc.ride_id
)

select * from final
