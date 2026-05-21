{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'usage_date',
            'data_type': 'date'
        },
        cluster_by=['promotion_id']
    )
}}

with fct_rides as (

    select * from {{ ref('fct_rides') }}

),

dim_promotion as (

    select
        promotion_id,
        promo_code,
        discount_type,
        discount_pct,
        discount_amount,
        max_discount_amount,
        min_fare_amount,
        usage_limit_total,
        usage_limit_per_user,
        valid_from,
        valid_to,
        promotion_status,
        is_currently_valid

    from {{ ref('dim_promotion') }}

),

promo_rides as (

    select
        promotion_id,
        ride_date                                               as usage_date,

        COUNT(ride_id)                                          as rides_with_promo,
        COUNT(DISTINCT rider_id)                                as unique_riders,

        SUM(total_promo_discount)                               as total_discount_applied,
        AVG(total_promo_discount)                               as avg_discount_applied,
        SUM(total_fare)                                         as gross_fare_with_promo,
        AVG(total_fare)                                         as avg_fare_with_promo,

        COUNTIF(is_completed)                                   as completed_promo_rides,
        COUNTIF(is_cancelled)                                   as cancelled_promo_rides,

        SAFE_DIVIDE(COUNTIF(is_completed), COUNT(ride_id))      as promo_completion_rate

    from fct_rides
    where has_promo = true
    group by 1, 2

),

final as (

    select
        pr.promotion_id,
        pr.usage_date,

        dp.promo_code,
        dp.discount_type,
        dp.discount_pct,
        dp.discount_amount,
        dp.max_discount_amount,
        dp.valid_from,
        dp.valid_to,
        dp.promotion_status,
        dp.is_currently_valid,

        pr.rides_with_promo,
        pr.unique_riders,
        pr.total_discount_applied,
        pr.avg_discount_applied,
        pr.gross_fare_with_promo,
        pr.avg_fare_with_promo,
        pr.completed_promo_rides,
        pr.cancelled_promo_rides,
        pr.promo_completion_rate

    from promo_rides       pr
    left join dim_promotion dp on pr.promotion_id = dp.promotion_id

)

select * from final
