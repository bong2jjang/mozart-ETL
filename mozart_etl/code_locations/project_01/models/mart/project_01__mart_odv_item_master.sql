{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01'),
    alias='mart_odv_item_master'
) }}

/*
    Output: ODV item_master

    이 모델은 staging에서 변환된 데이터를 mart 레이어로 가져옵니다.
    실제 ODV 테이블로 INSERT하려면 별도의 Trino 작업이 필요합니다.
*/

SELECT
    partition_key,
    project_id,
    plan_ver,
    item_id,
    item_type,
    item_name,
    item_group_id,
    description,
    procurement_type,
    item_spec,
    prod_type,
    item_size_type,
    item_priority,
    create_datetime,
    create_user_id,
    update_datetime,
    update_user_id
FROM {{ ref('project_01__stg_cfg_to_odv_item_master') }}
