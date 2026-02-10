{% macro limit_dates_for_dev(ref_date) -%}
{% if
    target.name == 'dev'
    and var("ignore_date_limits", false) is not true
-%}
    {{ref_date}} >= date_add('day', -{{var("dev_num_days_to_include", 90)}}, current_date)
{% else -%}
    TRUE
{%- endif %}
{%- endmacro %}
