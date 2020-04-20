{% macro drop_models(models) %}

{% for model in models %}
  {% set drop_model_sql %}
  DROP VIEW {{ target.schema }}.{{ model }}
  {% endset %}
  
  {{ dbt_utils.log_info(drop_model_sql) }}
  
  {{ run_query(drop_model_sql) }}
{% endfor %}

{% endmacro %}