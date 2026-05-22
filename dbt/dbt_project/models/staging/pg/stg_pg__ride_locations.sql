-- Expose 1 row per location_type as-is.
-- Pivoting to pickup_lat/lng, dropoff_lat/lng happens in int__ride_enriched.
with source as (
    select * from {{ source('dev_bronze_pg', 'ride_location') }}
),

final as (
    select
        ride_location_id,
        ride_id,
        location_type,
        latitude,
        longitude,
        address_text,
        place_id,
        captured_at,
        created_at
    from source
)

select * from final
