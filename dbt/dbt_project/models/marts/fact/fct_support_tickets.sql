{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ticket_sk',
    partition_by={'field': 'opened_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['ticket_status', 'priority', 'ticket_category']
) }}

with base as (
    select *, date(opened_at, 'Asia/Jakarta') as opened_date
    from {{ ref('stg_support_ticket') }}
    where not is_deleted
    {% if is_incremental() %}
      and date(updated_at, 'Asia/Jakarta') >= date_sub((select coalesce(max(opened_date), date('2024-01-01')) from {{ this }}), interval 3 day)
    {% endif %}
)
select
    {{ dbt_utils.generate_surrogate_key(['b.ticket_id']) }} as ticket_sk,
    b.ticket_id,
    fr.ride_sk,
    dr.rider_sk,
    dd.driver_sk,
    b.ride_id,
    b.rider_id,
    b.driver_id,
    b.ticket_category,
    b.ticket_status,
    b.priority,
    b.opened_at,
    b.resolved_at,
    b.created_at,
    b.updated_at,
    b.opened_date,
    timestamp_diff(b.resolved_at, b.opened_at, minute) as resolution_min,
    current_timestamp() as _dbt_loaded_at
from base b
left join {{ ref('fct_rides') }} fr using (ride_id)
left join {{ ref('dim_rider') }} dr
  on b.rider_id = dr.rider_id
 and b.opened_at >= dr.valid_from
 and b.opened_at < coalesce(dr.valid_to, timestamp('9999-12-31'))
left join {{ ref('dim_driver') }} dd
  on b.driver_id = dd.driver_id
 and b.opened_at >= dd.valid_from
 and b.opened_at < coalesce(dd.valid_to, timestamp('9999-12-31'))
