with users as (
    select * from {{ ref('stg_pg__users') }}
),

user_roles as (
    select
        ur.user_id,
        STRING_AGG(r.role_code order by r.role_code) as roles
    from {{ ref('stg_pg__user_roles') }} ur
    join {{ ref('stg_pg__roles') }} r using (role_id)
    where ur.is_active = true
    group by ur.user_id
),

lifetime as (
    select * from {{ ref('int__rider_lifetime') }}
),

final as (
    select
        u.user_id,
        u.username,
        u.email,
        u.phone_number,
        u.account_status,
        u.signup_date,
        u.created_at,
        u.updated_at,
        u.deleted_at,

        -- cohort_month computed here (not in staging)
        DATE_TRUNC(u.signup_date, MONTH)                                as cohort_month,

        -- role summary
        COALESCE(ur.roles, 'RIDER')                                     as roles,

        -- lifetime stats
        COALESCE(l.total_rides, 0)                                      as total_rides,
        COALESCE(l.completed_rides, 0)                                  as completed_rides,
        COALESCE(l.cancelled_rides, 0)                                  as cancelled_rides,
        COALESCE(l.total_spend, 0)                                      as total_spend,
        l.first_ride_at,
        l.last_ride_at,
        COALESCE(l.days_since_last_ride, 99999)                         as days_since_last_ride,
        l.avg_fare,
        l.favorite_service_type,

        -- segment
        CASE
            WHEN COALESCE(l.total_rides, 0) <= 1
                THEN 'new'
            WHEN COALESCE(l.days_since_last_ride, 99999) <= 30
                THEN 'active'
            WHEN COALESCE(l.days_since_last_ride, 99999) <= 90
                THEN 'at_risk'
            ELSE 'churned'
        END                                                             as segment

    from users u
    left join user_roles ur  on u.user_id = ur.user_id
    left join lifetime l     on u.user_id = l.user_id
)

select * from final
