with source as (
    select * from {{ source('dev_bronze_pg', 'review') }}
),

final as (
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
        {{ get_jakarta_date('created_at') }}     as review_date,
        rating_score >= 4                        as is_positive
    from source
)

select * from final
