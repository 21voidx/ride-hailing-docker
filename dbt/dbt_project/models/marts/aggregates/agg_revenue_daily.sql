{{
    config(
        materialized='table',
        partition_by={'field': 'revenue_date', 'data_type': 'date'}
    )
}}

with rides as (
    select
        city_code,
        service_type,
        ride_date,
        total_fare,
        platform_fee,
        driver_earning,
        has_promo,
        discount_amount,
        is_completed
    from {{ ref('fct_rides') }}
),

payments as (
    select
        p.txn_date,
        p.amount,
        p.net_amount,
        p.total_refunded,
        p.method_fee,
        p.is_paid,
        p.is_refunded,
        r.city_code,
        r.service_type
    from {{ ref('fct_payments') }} p
    left join {{ ref('fct_rides') }} r using (ride_id)
),

ride_agg as (
    select
        city_code,
        service_type,
        ride_date                                       as revenue_date,
        COALESCE(SUM(case when is_completed then total_fare end), 0)
                                                        as gross_revenue,
        COALESCE(SUM(case when is_completed then platform_fee end), 0)
                                                        as platform_fee_revenue,
        COALESCE(SUM(case when is_completed then driver_earning end), 0)
                                                        as driver_payout,
        COALESCE(SUM(case when has_promo then discount_amount end), 0)
                                                        as promo_discount_cost,
        COUNTIF(is_completed)                           as paid_rides
    from rides
    group by city_code, service_type, ride_date
),

payment_agg as (
    select
        city_code,
        service_type,
        txn_date                                        as revenue_date,
        COALESCE(SUM(case when is_paid then net_amount end), 0)
                                                        as collected_revenue,
        COALESCE(SUM(case when is_refunded then total_refunded end), 0)
                                                        as total_refunds,
        COALESCE(SUM(case when is_paid then method_fee end), 0)
                                                        as method_fee_cost,
        COUNTIF(is_paid)                                as paid_txns
    from payments
    group by city_code, service_type, txn_date
),

final as (
    select
        COALESCE(ra.city_code, pa.city_code)            as city_code,
        COALESCE(ra.service_type, pa.service_type)      as service_type,
        COALESCE(ra.revenue_date, pa.revenue_date)      as revenue_date,
        COALESCE(ra.gross_revenue, 0)                   as gross_revenue,
        COALESCE(ra.platform_fee_revenue, 0)            as platform_fee_revenue,
        COALESCE(ra.driver_payout, 0)                   as driver_payout,
        COALESCE(ra.promo_discount_cost, 0)             as promo_discount_cost,
        COALESCE(pa.collected_revenue, 0)               as collected_revenue,
        COALESCE(pa.total_refunds, 0)                   as total_refunds,
        COALESCE(pa.method_fee_cost, 0)                 as method_fee_cost,
        COALESCE(ra.gross_revenue, 0)
            - COALESCE(pa.total_refunds, 0)             as net_revenue,
        COALESCE(ra.paid_rides, 0)                      as paid_rides,
        COALESCE(pa.paid_txns, 0)                       as paid_txns
    from ride_agg ra
    full outer join payment_agg pa
               on ra.city_code    = pa.city_code
              and ra.service_type = pa.service_type
              and ra.revenue_date = pa.revenue_date
)

select
    {{ dbt_utils.generate_surrogate_key(['city_code', 'service_type', 'revenue_date']) }}
                                                        as agg_key,
    *
from final
