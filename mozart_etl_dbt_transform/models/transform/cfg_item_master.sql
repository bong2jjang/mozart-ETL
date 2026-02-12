{{ config(
    materialized='table',
    schema=var('tenant_id', 'default')
) }}

SELECT
    project_id,
    item_id,
    item_type,
    item_name,
    item_group_id,
    description,
    item_priority,
    procurement_type,
    prod_type,
    item_size_type,
    item_spec,
    create_datetime,
    update_datetime
FROM {{ source('raw', 'cfg_item_master') }}
{% if var('project_id', none) is not none %}
WHERE project_id = '{{ var("project_id") }}'
{% endif %}
