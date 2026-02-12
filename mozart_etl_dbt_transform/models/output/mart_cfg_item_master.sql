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
FROM {{ ref('cfg_item_master') }}
