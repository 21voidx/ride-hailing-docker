{{ config(cluster_by=['driver_status', 'verification_status']) }}

select
    d.driver_id,
    {{ ride_surrogate_key(['d.driver_id']) }} as driver_key,
    d.user_id,
    u.account_status,
    d.license_expiry,
    d.driver_status,
    d.verification_status,
    d.verified_at,
    d.suspended_at,
    d.rating_avg,
    d.rating_count,
    d.created_at,
    d.updated_at,
    {{ audit_columns() }}
from {{ ref('int_cdc__drivers_latest') }} d
left join {{ ref('stg_pg__user_accounts') }} u on d.user_id = u.user_id
where not d.is_deleted
