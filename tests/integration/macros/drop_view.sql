{% macro drop_view(model_name) %}

{% set drop_view_sql %}
DROP VIEW IF EXISTS {{ target.schema }}.{{ model_name }}
{% endset %}

{{ dbt_utils.log_info(drop_relation_sql) }}
{{ run_query(drop_view_sql) }}

{% endmacro %}
