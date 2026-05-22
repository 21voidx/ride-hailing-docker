with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="date_add(current_date(), interval 1 year)"
    ) }}
),

holidays as (
    select * from {{ ref('indonesia_public_holidays') }}
),

final as (
    select
        cast(date_day as DATE)                                          as date_day,
        CAST(EXTRACT(YEAR FROM date_day) AS INT64)                      as year,
        CAST(EXTRACT(QUARTER FROM date_day) AS INT64)                   as quarter,
        CAST(EXTRACT(MONTH FROM date_day) AS INT64)                     as month,
        CAST(EXTRACT(WEEK FROM date_day) AS INT64)                      as week_of_year,
        CAST(EXTRACT(DAYOFWEEK FROM date_day) AS INT64)                 as day_of_week,
        FORMAT_DATE('%A', date_day)                                     as day_name,
        EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)                      as is_weekend,
        h.holiday_date is not null                                      as is_public_holiday,
        h.holiday_name,
        CASE
            WHEN EXTRACT(QUARTER FROM date_day) = 1 THEN 'Q1'
            WHEN EXTRACT(QUARTER FROM date_day) = 2 THEN 'Q2'
            WHEN EXTRACT(QUARTER FROM date_day) = 3 THEN 'Q3'
            ELSE 'Q4'
        END                                                             as season
    from date_spine
    left join holidays h on cast(date_day as DATE) = h.holiday_date
)

select * from final
