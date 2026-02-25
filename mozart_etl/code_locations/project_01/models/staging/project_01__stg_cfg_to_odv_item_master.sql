{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01')
) }}

/*
    CFG → ODV 변환: item_master

    변환 규칙:
    - partition_key: {project_id}_{plan_ver} 형태로 생성
    - plan_ver: 변수로 지정 (기본값: V1.0)
    - item_type: '_ODV' 접미사 추가 (샘플 변환)
    - item_name: '[변환됨] ' 접두사 추가 (샘플 변환)
    - create/update_datetime: 현재 시간으로 교체
*/

WITH source AS (
    SELECT * FROM {{ source('project_01_raw', 'cfg_item_master') }}
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
        item_id,

        -- 샘플 변환: item_type에 접미사 추가
        COALESCE(item_type, 'UNKNOWN') || '_ODV' AS item_type,

        -- 샘플 변환: item_name에 접두사 추가
        '[변환됨] ' || COALESCE(item_name, item_id) AS item_name,

        -- 일반 컬럼 (1:1 복사)
        item_group_id,
        description,
        procurement_type,
        item_spec,
        prod_type,
        item_size_type,
        item_priority,

        -- 타임스탬프 (현재 시간으로 교체)
        CURRENT_TIMESTAMP AS create_datetime,
        create_user_id,
        CURRENT_TIMESTAMP AS update_datetime,
        update_user_id
    FROM source
)

SELECT * FROM transformed
