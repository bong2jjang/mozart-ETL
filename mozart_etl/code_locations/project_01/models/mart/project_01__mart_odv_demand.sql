{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01'),
    alias='mart_odv_demand'
) }}

/*
    Output: ODV demand

    이 모델은 staging에서 변환된 데이터를 mart 레이어로 가져옵니다.
*/

SELECT
    partition_key,
    project_id,
    plan_ver,
    demand_ver,
    demand_id,
    item_id,
    site_id,
    buffer_id,
    due_date,
    demand_qty,
    demand_priority,
    demand_type,
    demand_group_id,
    cust_id,
    max_lateness_day,
    max_earliness_day,
    description
FROM {{ ref('project_01__stg_cfg_to_odv_demand') }}
