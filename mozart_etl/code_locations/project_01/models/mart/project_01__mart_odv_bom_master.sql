{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01'),
    alias='mart_odv_bom_master'
) }}

/*
    Output: ODV bom_master

    이 모델은 staging에서 변환된 데이터를 mart 레이어로 가져옵니다.
*/

SELECT
    partition_key,
    project_id,
    plan_ver,
    bom_id,
    bom_type,
    bom_priority,
    eff_start_datetime,
    eff_end_datetime,
    demand_item_id,
    description,
    create_datetime,
    create_user_id,
    update_datetime,
    update_user_id,
    prop01,
    prop02,
    prop03
FROM {{ ref('project_01__stg_cfg_to_odv_bom_master') }}
