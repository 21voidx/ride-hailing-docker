with source as (

    select * from {{ source('bronze_pg', 'review') }}

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

        {{ get_jakarta_date('created_at') }}  as review_date,
        (rating_score >= 4)                   as is_positive,
        (comments is not null and TRIM(comments) != '') as has_comment,

        _ingested_at,
        _source_system

    from source
    where deleted_at is null

)

select * from renamed
