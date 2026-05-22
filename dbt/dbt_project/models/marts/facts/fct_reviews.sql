{{
    config(
        materialized='table'
    )
}}

with reviews as (
    select * from {{ ref('stg_pg__reviews') }}
    where deleted_at is null
),

rides as (
    select ride_id, city_code, service_type, ride_date
    from {{ ref('fct_rides') }}
),

final as (
    select
        rv.review_id,
        rv.ride_id,
        rv.reviewer_id,
        rv.reviewee_id,
        rv.review_type,
        rv.rating_score,
        rv.comments,
        rv.review_date,
        rv.is_positive,
        rv.created_at,
        rv.updated_at,
        -- ride context
        ri.city_code,
        ri.service_type,
        ri.ride_date
    from reviews rv
    left join rides ri on rv.ride_id = ri.ride_id
)

select * from final
