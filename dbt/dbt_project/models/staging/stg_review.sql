-- Staging: Review
-- Source    : Batch → dev_bronze_pg.review
-- Strategy  : Upsert. Satu baris per review_id (state terbaru).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'review') }}
),

renamed as (
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

        -- derived
        deleted_at is not null as is_deleted,
        rating_score >= 4      as is_positive,
        rating_score <= 2      as is_negative

    from source
    where review_id is not null
      and deleted_at is null
)

select * from renamed
