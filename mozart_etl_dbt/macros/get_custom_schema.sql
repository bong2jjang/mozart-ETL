{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {% if node.fqn[2:-1]|length == 0 or node.fqn | length <= 2 %}
            {{ default_schema }}
        {% else %}
            {% set prefix = node.fqn[2:-1]|join('_') %}
            {{ prefix | trim }}
        {% endif %}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
