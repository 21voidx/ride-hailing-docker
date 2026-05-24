-- Staging: Ride Status History
-- Source    : Batch → dev_bronze_pg.ride_status_history
-- Strategy  : Append-only. Setiap baris = 1 perubahan status.
--             Tidak perlu deduplication.
-- Notes     : Timestamps sudah bertipe TIMESTAMP (bukan STRING).

with source as (
    select * from {{ source('ride_ops_batch', 'ride_status_history') }}
),

renamed as (
    select
        ride_status_history_id,
        ride_id,
        old_status,
        new_status,
        changed_by_user_id,
        reason_code,
        reason_note,
        changed_at,
        created_at

    from source
    where ride_status_history_id is not null
)

select * from renamed
