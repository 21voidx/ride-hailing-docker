with source as (
    select
        review_id,
ride_id,
reviewer_id,
reviewee_id,
review_type,
rating_score,
comments,
created_at,
updated_at,
deleted_at,
_ingested_at,
_source_system
    from {{ source('bronze_pg', 'review') }}
),
deduped as (
    select *
    from source
    qualify row_number() over (partition by review_id order by _ingested_at desc) = 1
)
select
    *,
    {{ audit_columns() }}
from deduped
