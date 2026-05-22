{{ config(materialized='view') }}

with ranked as (
    select
        ride_id,
        {{ surrogate_key(["'ride'", 'ride_id']) }} as ride_key,
        location_type,
        cast(latitude as numeric) as latitude,
        cast(longitude as numeric) as longitude,
        address_text,
        place_id,
        captured_at,
        row_number() over (partition by ride_id, location_type order by captured_at desc, ride_location_id desc) as rn
    from {{ ref('stg_pg__ride_locations') }}
), filtered as (
    select * except(rn)
    from ranked
    where rn = 1
)
select
    ride_id,
    max(ride_key) as ride_key,
    max(if(location_type = 'PICKUP_REQUESTED', latitude, null)) as pickup_requested_latitude,
    max(if(location_type = 'PICKUP_REQUESTED', longitude, null)) as pickup_requested_longitude,
    max(if(location_type = 'PICKUP_REQUESTED', address_text, null)) as pickup_requested_address,
    max(if(location_type = 'DROPOFF_REQUESTED', latitude, null)) as dropoff_requested_latitude,
    max(if(location_type = 'DROPOFF_REQUESTED', longitude, null)) as dropoff_requested_longitude,
    max(if(location_type = 'DROPOFF_REQUESTED', address_text, null)) as dropoff_requested_address,
    max(if(location_type = 'PICKUP_ACTUAL', latitude, null)) as pickup_actual_latitude,
    max(if(location_type = 'PICKUP_ACTUAL', longitude, null)) as pickup_actual_longitude,
    max(if(location_type = 'DROPOFF_ACTUAL', latitude, null)) as dropoff_actual_latitude,
    max(if(location_type = 'DROPOFF_ACTUAL', longitude, null)) as dropoff_actual_longitude,
    {{ audit_columns() }}
from filtered
group by ride_id
