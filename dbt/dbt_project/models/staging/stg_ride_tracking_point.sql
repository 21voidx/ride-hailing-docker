-- Staging: Ride Tracking Point
-- Source    : Batch → dev_bronze_pg.ride_tracking_point
-- Strategy  : Append-only. High-volume (banyak GPS points per ride).
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).
--             Grain: 1 baris per GPS point recording.
--             Gunakan dengan hati-hati — volume sangat besar.
--             Umumnya dipakai untuk analisis rute, tidak masuk fct_rides langsung.

with source as (
    select * from {{ source('ride_ops_batch', 'ride_tracking_point') }}
),

renamed as (
    select
        tracking_point_id,
        ride_id,
        driver_id,
        latitude,
        longitude,
        speed_kmh,
        heading_degree,
        accuracy_meter,
        recorded_at,
        created_at

    from source
    where tracking_point_id is not null
)

select * from renamed
