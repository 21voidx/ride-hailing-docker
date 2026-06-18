-- models/marts/aggregates/agg_ride_funnel_steps_daily.sql

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "metric_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with base as (

    select
        requested_date as metric_date,
        city_code,
        service_type,

        count(distinct ride_id) as requested_rides,

        count(distinct case when accepted_at is not null then ride_id end) as accepted_rides,
        count(distinct case when arrived_at is not null then ride_id end) as arrived_rides,
        count(distinct case when started_at is not null then ride_id end) as started_rides,
        count(distinct case when ride_status = 'COMPLETED' then ride_id end) as completed_rides,
        count(distinct case when payment_status = 'PAID' then ride_id end) as paid_rides

    from {{ ref('fct_rides') }}

    {% if is_incremental() %}
        where requested_date >= date_sub(current_date('Asia/Jakarta'), interval 7 day)
    {% endif %}

    group by 1, 2, 3
),

steps as (

    select metric_date, city_code, service_type, 1 as step_number, '01 Ride Requested' as funnel_step, requested_rides as ride_count from base
    union all
    select metric_date, city_code, service_type, 2, '02 Driver Accepted', accepted_rides from base
    union all
    select metric_date, city_code, service_type, 3, '03 Driver Arrived', arrived_rides from base
    union all
    select metric_date, city_code, service_type, 4, '04 Ride Started', started_rides from base
    union all
    select metric_date, city_code, service_type, 5, '05 Ride Completed', completed_rides from base
    union all
    select metric_date, city_code, service_type, 6, '06 Payment Paid', paid_rides from base

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'metric_date',
            'city_code',
            'service_type',
            'step_number'
        ]) }} as ride_funnel_step_daily_sk,

        metric_date,
        city_code,
        service_type,
        step_number,
        funnel_step,
        ride_count,

        first_value(ride_count) over (
            partition by metric_date, city_code, service_type
            order by step_number
        ) as starting_ride_count,

        lag(ride_count) over (
            partition by metric_date, city_code, service_type
            order by step_number
        ) as previous_step_ride_count,

        safe_divide(
            ride_count,
            first_value(ride_count) over (
                partition by metric_date, city_code, service_type
                order by step_number
            )
        ) as conversion_from_start_rate,

        safe_divide(
            ride_count,
            lag(ride_count) over (
                partition by metric_date, city_code, service_type
                order by step_number
            )
        ) as conversion_from_previous_step_rate,

        1 - safe_divide(
            ride_count,
            lag(ride_count) over (
                partition by metric_date, city_code, service_type
                order by step_number
            )
        ) as dropoff_from_previous_step_rate,

        current_timestamp() as transformed_at

    from steps

)

select *
from final