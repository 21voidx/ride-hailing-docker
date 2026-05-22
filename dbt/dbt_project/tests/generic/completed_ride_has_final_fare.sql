{% test completed_ride_has_final_fare(model, status_column, final_fare_column) %}
select *
from {{ model }}
where upper({{ status_column }}) = 'COMPLETED'
  and {{ final_fare_column }} is null
{% endtest %}
