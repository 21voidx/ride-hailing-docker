-- Dimension: Payment Method
-- Kombinasi payment_method_type (reference) + user_payment_method (per user)
-- SCD Type 1
-- Materialized: table (batch refresh, daily)

{{
    config(
        materialized='table',
        cluster_by=['user_payment_method_id']
    )
}}

with user_payment_methods as (
    select * from {{ ref('stg_user_payment_method') }}
    where is_deleted = false
),

payment_method_types as (
    select * from {{ ref('stg_payment_method_type') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['upm.user_payment_method_id']) }} as payment_method_sk,

        -- natural key
        upm.user_payment_method_id,
        upm.user_id,

        -- method type (reference)
        pmt.payment_method_type_id,
        pmt.method_code,
        pmt.method_name,

        -- user-specific method details
        upm.provider_name,
        upm.masked_account,
        upm.expiry_month,
        upm.expiry_year,
        upm.is_default,
        upm.is_active,
        upm.payment_method_status,

        -- timestamps
        upm.created_at  as method_created_at,
        upm.updated_at  as method_updated_at

    from user_payment_methods upm
    left join payment_method_types pmt
        on upm.payment_method_type_id = pmt.payment_method_type_id
)

select * from final
