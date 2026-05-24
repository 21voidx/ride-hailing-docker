-- Dimension: User (Rider)
-- SCD Type 1 — overwrite on change
-- Materialized: table (batch refresh, daily)

{{
    config(
        materialized='table',
        cluster_by=['user_id']
    )
}}

with users as (
    select * from {{ ref('stg_user_account') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,

        -- natural key
        user_id,

        -- attributes
        username,
        email,
        phone_number,
        account_status,
        is_email_verified,
        is_phone_verified,
        is_deleted,

        -- timestamps
        email_verified_at,
        phone_verified_at,
        last_login_at,
        created_at                                          as user_created_at,
        updated_at                                          as user_updated_at,

        -- derived: cohort
        format_date('%Y-%m', date(created_at))             as cohort_month,
        format_date('%Y-Q%Q', date(created_at))            as cohort_quarter

    from users
)

select * from final
