with source as (

    select * from {{ source('bronze_pg', 'ride_location') }}

),

pivoted as (

    select
        ride_id,

        MAX(case when location_type = 'PICKUP'         then latitude  end) as pickup_latitude,
        MAX(case when location_type = 'PICKUP'         then longitude end) as pickup_longitude,
        MAX(case when location_type = 'PICKUP'         then address_text end) as pickup_address,
        MAX(case when location_type = 'PICKUP'         then place_id  end) as pickup_place_id,

        MAX(case when location_type = 'DROPOFF'        then latitude  end) as dropoff_latitude,
        MAX(case when location_type = 'DROPOFF'        then longitude end) as dropoff_longitude,
        MAX(case when location_type = 'DROPOFF'        then address_text end) as dropoff_address,
        MAX(case when location_type = 'DROPOFF'        then place_id  end) as dropoff_place_id,

        MAX(case when location_type = 'PICKUP_ACTUAL'  then latitude  end) as pickup_actual_latitude,
        MAX(case when location_type = 'PICKUP_ACTUAL'  then longitude end) as pickup_actual_longitude,
        MAX(case when location_type = 'PICKUP_ACTUAL'  then address_text end) as pickup_actual_address,

        MAX(case when location_type = 'DROPOFF_ACTUAL' then latitude  end) as dropoff_actual_latitude,
        MAX(case when location_type = 'DROPOFF_ACTUAL' then longitude end) as dropoff_actual_longitude,
        MAX(case when location_type = 'DROPOFF_ACTUAL' then address_text end) as dropoff_actual_address,

        MAX(captured_at) as captured_at,
        MAX(created_at)  as created_at

    from source
    group by 1

)

select * from pivoted
