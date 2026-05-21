{{
    config(
        materialized='table',
        tags=['daily'],
        partition_by={
            'field': 'review_date',
            'data_type': 'date'
        },
        cluster_by=['review_type']
    )
}}

with reviews as (

    select * from {{ ref('stg_pg__reviews') }}

),

dim_reviewer as (

    select rider_id, username, segment
    from {{ ref('dim_rider') }}

),

dim_reviewee as (

    select rider_id, username, segment
    from {{ ref('dim_rider') }}

),

fct_rides_ref as (

    select ride_id, ride_date, service_type, city_code
    from {{ ref('fct_rides') }}

),

final as (

    select
        r.review_id,
        r.ride_id,
        r.reviewer_id,
        r.reviewee_id,
        r.review_type,
        r.rating_score,
        r.has_comment,
        r.is_positive,
        r.review_date,
        r.created_at,
        r.updated_at,

        rev.username                            as reviewer_username,
        rev.segment                             as reviewer_segment,
        rvw.username                            as reviewee_username,
        rvw.segment                             as reviewee_segment,

        fr.service_type,
        fr.city_code,

        CURRENT_TIMESTAMP()                     as _dbt_loaded_at

    from reviews      r
    left join dim_reviewer  rev on r.reviewer_id = rev.rider_id
    left join dim_reviewee  rvw on r.reviewee_id = rvw.rider_id
    left join fct_rides_ref fr  on r.ride_id     = fr.ride_id

)

select * from final
