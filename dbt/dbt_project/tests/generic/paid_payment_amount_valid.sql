{% test paid_payment_amount_valid(model, status_column, amount_column) %}
select *
from {{ model }}
where upper({{ status_column }}) in ('PAID', 'CAPTURED', 'AUTHORIZED')
  and ({{ amount_column }} is null or {{ amount_column }} < 0)
{% endtest %}
