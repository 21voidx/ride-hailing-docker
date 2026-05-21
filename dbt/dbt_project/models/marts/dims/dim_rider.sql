{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

with users as (

    select * from {{ ref('stg_pg__users') }}

),

user_roles as (

    select
        ur.user_id,
        STRING_AGG(r.role_code ORDER BY r.role_code) as role_codes
    from {{ ref('stg_pg__user_roles') }} ur
    left join {{ ref('stg_pg__roles') }} r on ur.role_id = r.role_id
    where ur.is_active = true
    group by 1

),

lifetime as (

    select * from {{ ref('int__rider_lifetime') }}

),

segmented as (

    select
        u.user_id,
        u.rider_id,
        u.username,
        u.email,
        u.phone_number,
        u.account_status,
        u.signup_date,
        u.cohort_month,
        u.created_at,
        u.updated_at,

        ur.role_codes,

        IFNULL(l.total_rides, 0)        as total_rides,
        IFNULL(l.completed_rides, 0)    as completed_rides,
        IFNULL(l.cancelled_rides, 0)    as cancelled_rides,
        IFNULL(l.total_spend, 0)        as total_spend,
        l.avg_fare,
        l.first_ride_at,
        l.last_ride_at,
        l.days_since_last_ride,
        l.favorite_service_type,

        CASE
            WHEN IFNULL(l.total_rides, 0) = 0
                THEN 'new'
            WHEN IFNULL(l.days_since_last_ride, 9999) <= 30
             AND IFNULL(l.completed_rides, 0) >= 1
                THEN 'active'
            WHEN IFNULL(l.days_since_last_ride, 9999) BETWEEN 31 AND 90
                THEN 'at_risk'
            ELSE 'churned'
        END                             as segment

    from users          u
    left join user_roles ur on u.user_id = ur.user_id
    left join lifetime   l  on u.user_id = l.rider_id

)

select * from segmented
