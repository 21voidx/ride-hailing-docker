{{ config(materialized='view') }}

select
    ride_id,
    {{ surrogate_key(["'ride'", 'ride_id']) }} as ride_key,
    count(*) as review_count,
    avg(cast(rating_score as numeric)) as avg_rating_score,
    max(if(review_type = 'RIDER_TO_DRIVER', rating_score, null)) as rider_to_driver_rating_score,
    max(if(review_type = 'RIDER_TO_DRIVER', comments, null)) as rider_to_driver_comments,
    max(created_at) as last_review_at,
    {{ audit_columns() }}
from {{ ref('stg_pg__reviews') }}
where deleted_at is null
group by ride_id
