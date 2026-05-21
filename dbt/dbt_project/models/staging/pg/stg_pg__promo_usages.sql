with source as (

    select * from {{ source('bronze_pg', 'promo_usage') }}

),

renamed as (

    select
        promo_usage_id,
        promotion_id,
        ride_id,
        rider_id,
        discount_amount_applied,
        used_at,
        created_at,
        _ingested_at,
        _source_system

    from source

)

select * from renamed
