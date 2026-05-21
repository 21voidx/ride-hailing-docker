{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'revenue_date',
            'data_type': 'date'
        },
        cluster_by=['city_code', 'service_type']
    )
}}

with fct_rides as (

    select * from {{ ref('fct_rides') }}

),

fct_payments as (

    select * from {{ ref('fct_payments') }}

),

dim_date as (

    select date_day, is_weekend, is_public_holiday_id, year, quarter, month
    from {{ ref('dim_date') }}

),

ride_revenue as (

    select
        ride_date                                               as revenue_date,
        city_code,
        service_type,

        COUNT(ride_id)                                          as total_rides,
        COUNTIF(is_completed)                                   as completed_rides,

        SUM(case when is_completed then total_fare else 0 end)       as gross_revenue,
        SUM(case when is_completed then platform_fee else 0 end)     as platform_fee_revenue,
        SUM(case when is_completed then driver_earning else 0 end)   as driver_payouts,
        SUM(case when is_completed then surge_amount else 0 end)     as surge_revenue,
        SUM(case when is_completed then discount_amount else 0 end)  as total_discounts,
        SUM(case when is_completed then total_promo_discount else 0 end) as promo_discounts,
        SUM(case when is_completed then tax_amount else 0 end)       as tax_collected,

        AVG(case when is_completed then total_fare end)              as avg_fare,
        AVG(case when is_completed then surge_multiplier end)        as avg_surge_multiplier

    from fct_rides
    group by 1, 2, 3

),

payment_revenue as (

    select
        txn_date                                                as revenue_date,

        SUM(amount)                                             as total_txn_amount,
        SUM(net_amount)                                         as total_net_amount,
        SUM(total_refunded)                                     as total_refunds,
        SUM(method_fee)                                         as total_method_fees,
        COUNTIF(is_paid)                                        as paid_txn_count,
        COUNTIF(is_refunded)                                    as refunded_txn_count,
        COUNTIF(is_failed)                                      as failed_txn_count

    from fct_payments
    group by 1

),

final as (

    select
        rr.revenue_date,
        rr.city_code,
        rr.service_type,

        d.is_weekend,
        d.is_public_holiday_id,
        d.year,
        d.quarter,
        d.month,

        rr.total_rides,
        rr.completed_rides,
        rr.gross_revenue,
        rr.platform_fee_revenue,
        rr.driver_payouts,
        rr.surge_revenue,
        rr.total_discounts,
        rr.promo_discounts,
        rr.tax_collected,
        rr.avg_fare,
        rr.avg_surge_multiplier,

        IFNULL(pr.total_txn_amount, 0)      as total_txn_amount,
        IFNULL(pr.total_net_amount, 0)      as total_net_amount,
        IFNULL(pr.total_refunds, 0)         as total_refunds,
        IFNULL(pr.total_method_fees, 0)     as total_method_fees,
        IFNULL(pr.paid_txn_count, 0)        as paid_txn_count,
        IFNULL(pr.refunded_txn_count, 0)    as refunded_txn_count,
        IFNULL(pr.failed_txn_count, 0)      as failed_txn_count,

        rr.gross_revenue - IFNULL(pr.total_refunds, 0) as net_revenue

    from ride_revenue      rr
    left join payment_revenue pr on rr.revenue_date = pr.revenue_date
    left join dim_date        d  on rr.revenue_date = d.date_day

)

select * from final
