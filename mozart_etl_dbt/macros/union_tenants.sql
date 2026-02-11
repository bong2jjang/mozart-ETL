{% macro union_tenants(table_name) %}
    {%- set tenants = var('tenants', ['project_01', 'project_02']) -%}

    {% for tenant_id in tenants %}
        SELECT
            '{{ tenant_id }}' AS tenant_id,
            t.*
        FROM {{ source(tenant_id, table_name) }} AS t
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endmacro %}
