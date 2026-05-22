{% test completed_ride_has_completed_at(model, status_column, completed_at_column) %}
select *
from {{ model }}
where upper({{ status_column }}) = 'COMPLETED'
  and {{ completed_at_column }} is null
{% endtest %}
