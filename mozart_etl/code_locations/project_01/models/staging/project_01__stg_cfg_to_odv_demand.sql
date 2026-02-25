{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01')
) }}

/*
    CFG → ODV 변환: demand

    변환 규칙:
    - partition_key: {project_id}_{plan_ver} 형태로 생성
    - plan_ver: 변수로 지정 (기본값: V1.0)
    - demand_qty: 10% 증가 (샘플 변환)
    - demand_type: '_PLANNED' 접미사 추가 (샘플 변환)
*/

WITH source AS (
    SELECT * FROM {{ source('project_01_raw', 'cfg_demand') }}
    {% if var('project_id', none) is not none %}
    WHERE project_id = '{{ var("project_id") }}'
    {% endif %}
),

transformed AS (
    SELECT
        -- ODV 전용 컬럼 (신규 생성)
        project_id || '_' || '{{ var("plan_ver", "V1.0") }}' AS partition_key,
        project_id,
        '{{ var("plan_ver", "V1.0") }}' AS plan_ver,

        -- PK
        demand_ver,
        demand_id,
        item_id,
        site_id,
        buffer_id,
        due_date,

        -- 샘플 변환: demand_qty 10% 증가
        demand_qty * 1.1 AS demand_qty,

        -- 일반 컬럼 (1:1 복사)
        demand_priority,

        -- 샘플 변환: demand_type에 접미사 추가
        COALESCE(demand_type, 'NORMAL') || '_PLANNED' AS demand_type,

        demand_group_id,
        cust_id,
        max_lateness_day,
        max_earliness_day,
        description
    FROM source
)

SELECT * FROM transformed
