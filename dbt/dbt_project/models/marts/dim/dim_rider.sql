{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['rider_id', 'valid_from']) }} as rider_sk,
    rider_snapshot_id,
    rider_id,
    username,
    full_name,
    email,
    phone_number,
    account_status,
    city_code,
    date(created_at) as rider_created_date,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ ref('stg_rider_account') }}
