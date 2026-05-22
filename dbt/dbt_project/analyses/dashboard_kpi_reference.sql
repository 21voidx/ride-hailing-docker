-- Optional analysis query for validating Looker KPI numbers against the reporting table.
select
  requested_date,
  count(*) as requested_rides,
  countif(is_completed) as completed_rides,
  safe_divide(countif(is_completed), count(*)) as completion_rate,
  sum(coalesce(total_fare, 0)) as gross_booking_value,
  sum(coalesce(platform_fee, 0)) as platform_revenue
from {{ ref('rpt_looker__ride_operations_dashboard') }}
group by requested_date
order by requested_date desc
