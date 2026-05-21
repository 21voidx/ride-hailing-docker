{{
    config(
        materialized='incremental',
        unique_key='ride_status_history_id',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'changed_date',
            'data_type': 'date'
        },
        cluster_by=['new_status']
    )
}}

with transitions as (

    select * from {{ ref('int__ride_status_transitions') }}

    {% if is_incremental() %}
    where changed_at > (
        select coalesce(MAX(changed_at), '2020-01-01')
        from {{ this }}
    )
    {% endif %}

),

fct_rides_ref as (

    select
        ride_id,
        rider_id,
        driver_id,
        service_type,
        city_code,
        ride_date
    from {{ ref('fct_rides') }}

),

final as (

    select
        t.ride_status_history_id,
        t.ride_id,
        t.old_status,
        t.new_status,
        t.changed_by_user_id,
        t.reason_code,
        t.reason_note,
        t.changed_at,
        t.changed_date,
        t.created_at,
        t.prev_status_changed_at,
        t.duration_seconds_in_prev_status,
        t.is_anomaly,

        fr.rider_id,
        fr.driver_id,
        fr.service_type,
        fr.city_code,
        fr.ride_date,

        CURRENT_TIMESTAMP()     as _dbt_loaded_at

    from transitions t
    left join fct_rides_ref fr on t.ride_id = fr.ride_id

)

select * from final
