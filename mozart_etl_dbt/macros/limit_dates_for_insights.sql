{% macro limit_dates_for_insights(ref_date) -%}
{{ref_date}} >= date_add('day', -{{var("insights_num_days_to_include", 130)}}, current_date)
{%- endmacro %}
