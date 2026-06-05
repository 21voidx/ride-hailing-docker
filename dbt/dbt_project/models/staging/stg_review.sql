select
    cast(review_id as int64) as review_id,
    cast(ride_id as int64) as ride_id,
    upper(cast(reviewer_type as string)) as reviewer_type,
    cast(reviewer_id as int64) as reviewer_id,
    upper(cast(reviewee_type as string)) as reviewee_type,
    cast(reviewee_id as int64) as reviewee_id,
    cast(rating_score as int64) as rating_score,
    cast(comments as string) as comments,
    upper(cast(review_status as string)) as review_status,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(deleted_at as timestamp) as deleted_at,
    deleted_at is not null as is_deleted
from {{ source('bronze_mysql', 'review') }}
