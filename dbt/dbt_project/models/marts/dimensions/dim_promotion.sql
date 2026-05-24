-- Dimension: Promotion
-- SCD Type 1
-- Materialized: table (batch refresh, daily)

{{
    config(
        materialized='table',
        cluster_by=['promotion_id']
    )
}}

with promos as (
    select * from {{ ref('stg_promotion') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['promotion_id']) }} as promotion_sk,

        -- natural key
        promotion_id,

        -- attributes
        promo_code,
        promo_description,
        discount_type,
        discount_pct,
        discount_amount,
        max_discount_amount,
        min_fare_amount,
        usage_limit_total,
        usage_limit_per_user,
        promotion_status,
        is_active,
        is_percent_discount,

        -- validity window
        valid_from,
        valid_to,
        promo_duration_days,

        created_at  as promo_created_at,
        updated_at  as promo_updated_at

    from promos
)

select * from final
