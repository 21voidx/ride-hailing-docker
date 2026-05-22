with source as (
    select * from {{ source('dev_bronze_pg', 'ride_status_history') }}
),

final as (
    select
        ride_status_history_id,
        ride_id,
        old_status,
        new_status,
        changed_by_user_id,
        reason_code,
        reason_note,
        changed_at,
        created_at,
        {{ get_jakarta_date('changed_at') }}     as changed_date
    from source
)

select * from final
