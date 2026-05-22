{{
    config(
        materialized='table',
        partition_by={'field': 'usage_date', 'data_type': 'date'}
    )
}}

with rides as (
    select
        promotion_id,
        ride_date                                           as usage_date,
        promo_discount_amount,
        total_fare,
        is_completed,
        rider_id
    from {{ ref('fct_rides') }}
    where has_promo = true
),

promotions as (
    select promotion_id, promo_code, discount_type, discount_pct, discount_amount, promotion_status
    from {{ ref('dim_promotion') }}
),

agg as (
    select
        r.promotion_id,
        r.usage_date,
        COUNT(*)                                            as usage_count,
        COUNTIF(r.is_completed)                             as completed_rides_with_promo,
        COUNT(DISTINCT r.rider_id)                          as unique_riders,
        COALESCE(SUM(r.promo_discount_amount), 0)           as total_discount_applied,
        COALESCE(AVG(r.promo_discount_amount), 0)           as avg_discount_per_ride,
        COALESCE(SUM(r.total_fare), 0)                      as gross_revenue_with_promo
    from rides r
    group by r.promotion_id, r.usage_date
)

select
    {{ dbt_utils.generate_surrogate_key(['promotion_id', 'usage_date']) }}
                                                            as agg_key,
    a.*,
    p.promo_code,
    p.discount_type,
    p.discount_pct,
    p.discount_amount                                       as promo_discount_flat_amount,
    p.promotion_status
from agg a
left join promotions p on a.promotion_id = p.promotion_id
