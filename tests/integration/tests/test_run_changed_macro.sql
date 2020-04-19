{% set model_test_query %}
select *
from {{ ref('test_model') }}
{% endset %}

{% set results = dbt_utils.get_query_results_as_dict(model_test_query) %}
{{ log(results, info=True) }}

{{ assert_equal (results | trim, "{'MY_INTEGER_COL': (Decimal('2'),)}" | trim) }}
