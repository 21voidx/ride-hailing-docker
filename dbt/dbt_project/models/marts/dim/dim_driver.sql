{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['driver_id', 'valid_from']) }} as driver_sk,
    driver_snapshot_id,
    driver_id,
    driver_name,
    phone_number,
    city_code,
    driver_status,
    verification_status,
    rating_avg,
    rating_count,
    date(created_at) as driver_created_date,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ ref('stg_driver_profile') }}
