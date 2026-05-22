-- Assert: customer-facing fare components must reconcile with FINAL total_fare.
--
-- Important:
-- 1. Discount, promo, and voucher components are treated as negative values.
-- 2. Driver payout, driver earning, commission, platform fee, and payment/provider fee
--    rows are ignored because they are revenue allocation or cost rows, not rider-facing
--    fare components.
-- 3. Tolerance is 1 IDR to allow rounding differences.

with final_fares as (
    select
        cast(fare_id as INT64)                                  as fare_id,
        cast(ride_id as INT64)                                  as ride_id,
        cast(total_fare as NUMERIC)                             as total_fare
    from {{ ref('stg_pg__ride_fares') }}
    where fare_type = 'FINAL'
),

prepared_components as (
    select
        cast(fare_id as INT64)                                  as fare_id,
        upper(trim(cast(component_code as STRING)))             as component_code,
        safe_cast(component_amount as NUMERIC)                  as component_amount
    from {{ ref('stg_pg__ride_fare_components') }}
),

component_totals as (
    select
        fare_id,
        sum(
            case
                -- Allocation or cost components. These should not be added into rider total_fare.
                when component_code like '%DRIVER%'
                  or component_code like '%EARNING%'
                  or component_code like '%PAYOUT%'
                  or component_code like '%COMMISSION%'
                  or component_code like '%PLATFORM_FEE%'
                  or component_code like '%METHOD_FEE%'
                  or component_code like '%PAYMENT_FEE%'
                  or component_code like '%PROVIDER_FEE%'
                    then 0

                -- Aggregate components. These would double count if included with itemized rows.
                when component_code in ('TOTAL', 'TOTAL_FARE', 'GRAND_TOTAL', 'SUBTOTAL', 'NET_TOTAL')
                    then 0

                -- Discounts reduce the customer-facing fare.
                when component_code like '%DISCOUNT%'
                  or component_code like '%PROMO%'
                  or component_code like '%VOUCHER%'
                    then -abs(coalesce(component_amount, 0))

                -- Normal rider-facing fare components.
                else coalesce(component_amount, 0)
            end
        )                                                       as component_sum,
        string_agg(
            distinct component_code,
            ', ' order by component_code
        )                                                       as component_codes
    from prepared_components
    group by fare_id
),

validation as (
    select
        ff.ride_id,
        ff.fare_id,
        ff.total_fare,
        coalesce(ct.component_sum, 0)                           as component_sum,
        abs(ff.total_fare - coalesce(ct.component_sum, 0))       as diff,
        ct.component_codes
    from final_fares ff
    left join component_totals ct
           on ff.fare_id = ct.fare_id
)

select *
from validation
where diff > 1