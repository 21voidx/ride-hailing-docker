with review_latest as (
    select * except(rn)
    from (
        select rv.*, row_number() over (partition by ride_id order by updated_at desc, review_id desc) as rn
        from {{ ref('stg_review') }} rv
        where not is_deleted and review_status = 'PUBLISHED'
    )
    where rn = 1
),
ticket_latest as (
    select * except(rn)
    from (
        select st.*, row_number() over (partition by ride_id order by updated_at desc, ticket_id desc) as rn
        from {{ ref('stg_support_ticket') }} st
        where not is_deleted
    )
    where rn = 1
)
select
    coalesce(r.ride_id, t.ride_id) as ride_id,
    r.review_id,
    r.rating_score,
    r.comments,
    r.created_at as reviewed_at,
    t.ticket_id,
    t.ticket_category,
    t.ticket_status,
    t.priority,
    t.opened_at,
    t.resolved_at,
    timestamp_diff(t.resolved_at, t.opened_at, minute) as ticket_resolution_min
from review_latest r
full outer join ticket_latest t using (ride_id)
