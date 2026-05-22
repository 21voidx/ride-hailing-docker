{{ config(
    unique_key='driver_id',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['driver_id', 'is_deleted']
) }}

with source_events as (
    select *
    from {{ ref('stg_cdc__driver_profile_events') }}
    {% if is_incremental() %}
      where cdc_event_at >= coalesce(
        (select timestamp_sub(max(cdc_event_at), interval 2 hour) from {{ this }}),
        timestamp('1970-01-01')
      )
    {% endif %}
),
latest as (
    select *
    from source_events
    qualify row_number() over (
        partition by driver_id
        order by cdc_event_at desc, cdc_lsn desc, cdc_partition_at desc
    ) = 1
)
select
    *,
    date(coalesce(created_at, cdc_event_at), '{{ var("timezone", "Asia/Jakarta") }}') as event_date,
    lower(cdc_operation) = 'd' as is_deleted
from latest
