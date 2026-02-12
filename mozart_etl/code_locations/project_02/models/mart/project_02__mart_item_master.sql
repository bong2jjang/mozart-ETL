{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_02'),
    alias='mart_item_master'
) }}

-- 제품 표준 스키마: mart_item_master
SELECT
    item_id::VARCHAR          AS item_id,
    item_name::VARCHAR        AS item_name,
    item_type::VARCHAR        AS item_type,
    item_group_id::VARCHAR    AS item_group_id,
    procurement_type::VARCHAR AS procurement_type,
    create_datetime           AS created_at,
    update_datetime           AS updated_at
FROM {{ ref('project_02__stg_cfg_item_master') }}
