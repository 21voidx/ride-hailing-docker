{% snapshot snap_rider_account %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='rider_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_pg', 'rider_account') }}
{% endsnapshot %}

{% snapshot snap_driver_profile %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='driver_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_pg', 'driver_profile') }}
{% endsnapshot %}

{% snapshot snap_vehicle %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='vehicle_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_pg', 'vehicle') }}
{% endsnapshot %}

{% snapshot snap_driver_vehicle_assignment %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='assignment_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_pg', 'driver_vehicle_assignment') }}
{% endsnapshot %}

{% snapshot snap_payment_method %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='payment_method_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_mysql', 'payment_method') }}
{% endsnapshot %}

{% snapshot snap_promotion %}
{{ config(target_schema=env_var('BQ_SNAPSHOT_DATASET', 'snapshots_ride_hailing'), unique_key='promotion_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select * from {{ source('bronze_mysql', 'promotion') }}
{% endsnapshot %}
