{{
    config(
        materialized='incremental',
        unique_key='rider_id',
        on_schema_change='sync_all_columns'
    )
}}

with ride_enriched as (

    select * from {{ ref('int__ride_enriched') }}

    {% if is_incremental() %}
    where __source_ts_ms > (
        select coalesce(MAX(last_updated_ts_ms), 0)
        from {{ this }}
    )
    {% endif %}

),

agg as (

    select
        rider_id,

        COUNT(*)                                                     as total_rides,
        COUNTIF(is_completed)                                        as completed_rides,
        COUNTIF(is_cancelled)                                        as cancelled_rides,
        COUNTIF(has_promo)                                           as promo_rides,

        SUM(case when is_completed then total_fare else 0 end)       as total_spend,
        AVG(case when is_completed then total_fare end)              as avg_fare,

        MIN(requested_at)                                            as first_ride_at,
        MAX(requested_at)                                            as last_ride_at,

        DATE_DIFF(
            CURRENT_DATE('Asia/Jakarta'),
            MAX(ride_date),
            DAY
        )                                                            as days_since_last_ride,

        (
            select service_type
            from (
                select service_type, COUNT(*) as cnt
                from ride_enriched inner_r
                where inner_r.rider_id = ride_enriched.rider_id
                  and inner_r.is_completed
                group by 1
                order by cnt desc
                limit 1
            )
        )                                                            as favorite_service_type,

        MAX(__source_ts_ms)                                          as last_updated_ts_ms

    from ride_enriched
    group by 1

)

select * from agg
