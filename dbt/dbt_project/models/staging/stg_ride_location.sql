-- Staging: Ride Location
-- Source    : Batch → dev_bronze_pg.ride_location
-- Strategy  : Append-only. Setiap baris = 1 koordinat per tipe lokasi per ride.
--             Tidak perlu deduplication.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'ride_location') }}
),

renamed as (
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
    where ride_location_id is not null
)

select * from renamed
