{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ var('start_date') ~ "' as date)",
            end_date="date_add(current_date(), interval 1 year)"
        )
    }}

),

indonesia_public_holidays as (

    select holiday_date
    from UNNEST(ARRAY<DATE>[
        -- 2020
        DATE '2020-01-01', DATE '2020-01-25', DATE '2020-02-15', DATE '2020-03-22',
        DATE '2020-03-25', DATE '2020-04-09', DATE '2020-04-10', DATE '2020-04-24',
        DATE '2020-05-01', DATE '2020-05-07', DATE '2020-05-20', DATE '2020-05-21',
        DATE '2020-05-24', DATE '2020-06-01', DATE '2020-07-31', DATE '2020-08-17',
        DATE '2020-08-20', DATE '2020-09-30', DATE '2020-10-29', DATE '2020-12-25',
        -- 2021
        DATE '2021-01-01', DATE '2021-02-12', DATE '2021-03-11', DATE '2021-03-14',
        DATE '2021-04-02', DATE '2021-05-01', DATE '2021-05-13', DATE '2021-05-14',
        DATE '2021-05-15', DATE '2021-05-16', DATE '2021-05-20', DATE '2021-05-26',
        DATE '2021-06-01', DATE '2021-07-20', DATE '2021-08-17', DATE '2021-08-18',
        DATE '2021-10-19', DATE '2021-12-25',
        -- 2022
        DATE '2022-01-01', DATE '2022-02-01', DATE '2022-02-28', DATE '2022-03-03',
        DATE '2022-04-15', DATE '2022-04-29', DATE '2022-05-01', DATE '2022-05-02',
        DATE '2022-05-03', DATE '2022-05-04', DATE '2022-05-26', DATE '2022-06-01',
        DATE '2022-07-09', DATE '2022-08-17', DATE '2022-07-30', DATE '2022-10-08',
        DATE '2022-10-25', DATE '2022-12-25',
        -- 2023
        DATE '2023-01-01', DATE '2023-01-22', DATE '2023-01-23', DATE '2023-02-18',
        DATE '2023-03-22', DATE '2023-03-23', DATE '2023-04-07', DATE '2023-04-21',
        DATE '2023-04-22', DATE '2023-04-23', DATE '2023-04-24', DATE '2023-04-25',
        DATE '2023-05-01', DATE '2023-05-18', DATE '2023-06-01', DATE '2023-06-02',
        DATE '2023-06-04', DATE '2023-06-18', DATE '2023-06-19', DATE '2023-07-19',
        DATE '2023-08-17', DATE '2023-09-27', DATE '2023-10-28', DATE '2023-12-25',
        DATE '2023-12-26',
        -- 2024
        DATE '2024-01-01', DATE '2024-02-08', DATE '2024-02-09', DATE '2024-02-10',
        DATE '2024-03-11', DATE '2024-03-29', DATE '2024-04-08', DATE '2024-04-09',
        DATE '2024-04-10', DATE '2024-04-11', DATE '2024-04-12', DATE '2024-05-01',
        DATE '2024-05-09', DATE '2024-05-23', DATE '2024-06-01', DATE '2024-06-17',
        DATE '2024-06-18', DATE '2024-07-07', DATE '2024-08-17', DATE '2024-09-16',
        DATE '2024-10-05', DATE '2024-12-25', DATE '2024-12-26',
        -- 2025
        DATE '2025-01-01', DATE '2025-01-27', DATE '2025-01-28', DATE '2025-01-29',
        DATE '2025-02-15', DATE '2025-03-29', DATE '2025-03-30', DATE '2025-03-31',
        DATE '2025-04-01', DATE '2025-04-07', DATE '2025-04-18', DATE '2025-05-01',
        DATE '2025-05-12', DATE '2025-05-29', DATE '2025-06-01', DATE '2025-06-06',
        DATE '2025-06-07', DATE '2025-08-17', DATE '2025-09-05', DATE '2025-09-25',
        DATE '2025-10-02', DATE '2025-12-25', DATE '2025-12-26',
        -- 2026
        DATE '2026-01-01', DATE '2026-02-17', DATE '2026-03-19', DATE '2026-03-20',
        DATE '2026-04-03', DATE '2026-04-06', DATE '2026-04-07', DATE '2026-04-08',
        DATE '2026-05-01', DATE '2026-05-14', DATE '2026-05-21', DATE '2026-06-01',
        DATE '2026-06-26', DATE '2026-06-27', DATE '2026-08-17', DATE '2026-09-15',
        DATE '2026-10-14', DATE '2026-12-25'
    ]) as holiday_date

),

final as (

    select
        cast(date_day as date)                                   as date_day,
        EXTRACT(YEAR   FROM date_day)                            as year,
        EXTRACT(QUARTER FROM date_day)                           as quarter,
        EXTRACT(MONTH  FROM date_day)                            as month,
        EXTRACT(WEEK   FROM date_day)                            as week_of_year,
        EXTRACT(DAYOFWEEK FROM date_day)                         as day_of_week,
        FORMAT_DATE('%A', date_day)                              as day_name,
        FORMAT_DATE('%B', date_day)                              as month_name,
        DATE_TRUNC(date_day, WEEK)                               as week_start,
        DATE_TRUNC(date_day, MONTH)                              as month_start,
        DATE_TRUNC(date_day, QUARTER)                            as quarter_start,
        DATE_TRUNC(date_day, YEAR)                               as year_start,

        EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)              as is_weekend,

        (h.holiday_date is not null)                             as is_public_holiday_id,

        CASE
            WHEN EXTRACT(QUARTER FROM date_day) = 1 THEN 'Q1 (Jan–Mar)'
            WHEN EXTRACT(QUARTER FROM date_day) = 2 THEN 'Q2 (Apr–Jun)'
            WHEN EXTRACT(QUARTER FROM date_day) = 3 THEN 'Q3 (Jul–Sep)'
            ELSE 'Q4 (Oct–Dec)'
        END                                                      as season

    from date_spine
    left join indonesia_public_holidays h on cast(date_day as date) = h.holiday_date

)

select * from final
