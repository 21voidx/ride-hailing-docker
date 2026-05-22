{% test duration_non_negative(model, start_column, end_column) %}
select *
from {{ model }}
where {{ start_column }} is not null
  and {{ end_column }} is not null
  and {{ end_column }} < {{ start_column }}
{% endtest %}
