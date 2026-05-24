-- Aggregate Fact: Rides Daily
-- Grain: 1 row per ride_date + city_code + service_type
-- Strategy: insert_overwrite by daily partition

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'ride_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['city_code', 'service_type'],
        on_schema_change='sync_all_columns'
    )
}}

with rides as (
    select *
    from {{ ref('fct_rides') }}
    {% if is_incremental() %}
        where ride_date >= date_sub(current_date('{{ var("reporting_timezone", "Asia/Jakarta") }}'), interval {{ var('incremental_lookback_days', 2) }} day)
    {% endif %}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ride_date', 'city_code', 'service_type']) }} as ride_daily_sk,
        ride_date,
        ride_date_key,
        city_code,
        service_type,

        count(*) as ride_count,
        countif(is_completed) as completed_ride_count,
        countif(is_cancelled) as cancelled_ride_count,
        countif(is_payment_failed) as payment_failed_ride_count,
        countif(is_promo_used) as promo_ride_count,
        countif(is_surge) as surge_ride_count,
        countif(has_rider_review) as rider_review_count,

        sum(total_fare) as total_fare,
        sum(platform_fee) as platform_fee,
        sum(driver_earning) as driver_earning,
        sum(fare_discount_amount) as fare_discount_amount,
        sum(promo_discount_applied) as promo_discount_applied,
        sum(selected_total_charged) as selected_total_charged,
        sum(gross_total_charged) as gross_total_charged,

        avg(wait_to_accept_sec) as avg_wait_to_accept_sec,
        avg(pickup_travel_sec) as avg_pickup_travel_sec,
        avg(pickup_wait_sec) as avg_pickup_wait_sec,
        avg(ride_duration_sec) as avg_ride_duration_sec,
        avg(total_elapsed_sec) as avg_total_elapsed_sec,
        avg(actual_distance_km) as avg_actual_distance_km,
        avg(actual_duration_min) as avg_actual_duration_min,
        avg(rider_rating_given) as avg_rider_rating_given,

        safe_divide(countif(is_completed), count(*)) as completion_rate,
        safe_divide(countif(is_cancelled), count(*)) as cancellation_rate,
        safe_divide(sum(platform_fee), nullif(sum(total_fare), 0)) as platform_take_rate,

        max(_last_source_updated_at) as _last_source_updated_at,
        current_timestamp() as _dbt_loaded_at
    from rides
    group by 1, 2, 3, 4, 5
)

select * from final
