-- Staging: Ride Fare Component
-- Source    : Batch → dev_bronze_pg.ride_fare_component
-- Strategy  : Append-only. Breakdown komponen fare per fare_id.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).
--             Grain: 1 baris per fare_id + component_code.

with source as (
    select * from {{ source('ride_ops_batch', 'ride_fare_component') }}
),

renamed as (
    select
        fare_component_id,
        fare_id,
        component_code,
        component_name,
        component_amount,
        description,
        created_at

    from source
    where fare_component_id is not null
)

select * from renamed
