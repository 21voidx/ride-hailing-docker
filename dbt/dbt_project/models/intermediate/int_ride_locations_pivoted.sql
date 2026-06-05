-- Intermediate: pivot ride_location dari beberapa rows per ride menjadi 1 row per ride
-- Dipakai oleh fct_rides
-- Materialized sebagai ephemeral (CTE inline)
-- Grain: 1 row per ride_id
-- Notes: Jika ada duplikasi per ride_id + location_type dari batch re-load,
--        ambil record terbaru agar lat/lng/address tidak tercampur dari row berbeda.

with locations as (
    select * from {{ ref('stg_ride_location') }}
),

latest_locations as (
    select *
    from locations
    qualify row_number() over (
        partition by ride_id, location_type
        order by captured_at desc, created_at desc, ride_location_id desc
    ) = 1
),

pivoted as (
    select
        ride_id,

        -- Pickup (requested)
        max(case when location_type = 'PICKUP_REQUESTED'
            then latitude end)     as pickup_req_lat,
        max(case when location_type = 'PICKUP_REQUESTED'
            then longitude end)    as pickup_req_lng,
        max(case when location_type = 'PICKUP_REQUESTED'
            then address_text end) as pickup_req_address,

        -- Dropoff (requested)
        max(case when location_type = 'DROPOFF_REQUESTED'
            then latitude end)     as dropoff_req_lat,
        max(case when location_type = 'DROPOFF_REQUESTED'
            then longitude end)    as dropoff_req_lng,
        max(case when location_type = 'DROPOFF_REQUESTED'
            then address_text end) as dropoff_req_address,

        -- Pickup (actual)
        max(case when location_type = 'PICKUP_ACTUAL'
            then latitude end)     as pickup_actual_lat,
        max(case when location_type = 'PICKUP_ACTUAL'
            then longitude end)    as pickup_actual_lng,

        -- Dropoff (actual)
        max(case when location_type = 'DROPOFF_ACTUAL'
            then latitude end)     as dropoff_actual_lat,
        max(case when location_type = 'DROPOFF_ACTUAL'
            then longitude end)    as dropoff_actual_lng

    from latest_locations
    group by ride_id
)

select * from pivoted
