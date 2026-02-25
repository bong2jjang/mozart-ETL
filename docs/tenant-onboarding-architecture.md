# 테넌트 온보딩 아키텍처

> 신규 테넌트 추가 시 `code_locations/{tenant_id}/` 하위만 변경하고,
> 그 외 파일은 `sync_tenants.py`가 자동 생성한다.
> 각 테넌트는 서로 다른 소스 테이블을 가지지만, mart 출력은 제품 표준 스키마로 통일된다.

---

## 1. 파이프라인 흐름

```
테넌트 A: erp_items         ──staging──▶  mart_item_master  (표준)
테넌트 B: product_master    ──staging──▶  mart_item_master  (표준)
테넌트 C: inv_products      ──staging──▶  mart_item_master  (표준)
```

| 단계 | 특성 | Asset Key |
|------|------|-----------|
| **INPUT** | 테넌트마다 소스 DB 스키마가 다름 | `[tid, "input", table]` |
| **STAGING** | 테넌트별 고유 SQL로 정규화 | `[tid, "staging", stg_model]` |
| **OUTPUT** | 모든 테넌트가 제품 표준 스키마로 동일한 mart 생성 | `[tid, "output", mart_model]` |

```
project_01/input/cfg_item_master  → project_01/staging/stg_cfg_item_master → project_01/output/mart_item_master
project_02/input/product_master   → project_02/staging/stg_product_master  → project_02/output/mart_item_master
                                                                              ↑ 모두 같은 스키마
```

---

## 2. 디렉토리 구조

```
mozart-etl/
├── workspace.yaml                         # ★ 자동 생성
├── Makefile                               # dev, sync, validate 명령
├── scripts/
│   └── sync_tenants.py                    # ★ 자동 생성 스크립트
│
├── mozart_etl/
│   └── code_locations/
│       ├── _shared.py                     # 공통 리소스 (고정)
│       ├── _tenant_factory.py             # 팩토리 (고정)
│       │
│       ├── project_01/                    # ── 테넌트 A ──
│       │   ├── tenant.yaml                #   설정 (수동)
│       │   ├── __init__.py                #   ★ 자동 생성
│       │   └── models/                    #   dbt 모델 (수동)
│       │       ├── _sources.yml
│       │       ├── staging/
│       │       │   └── project_01__stg_cfg_item_master.sql
│       │       └── mart/
│       │           └── project_01__mart_item_master.sql
│       │
│       └── project_02/                    # ── 테넌트 B ──
│           ├── tenant.yaml
│           ├── __init__.py                #   ★ 자동 생성
│           └── models/
│               ├── _sources.yml
│               ├── staging/
│               │   └── project_02__stg_cfg_item_master.sql
│               └── mart/
│                   └── project_02__mart_item_master.sql
│
├── mozart_etl_dbt_transform/              # 공유 dbt 프로젝트
│   ├── dbt_project.yml                    # ★ 자동 생성 (model-paths)
│   ├── profiles.yml                       # Trino 연결 (고정)
│   ├── macros/generate_schema_name.sql    # (고정)
│   ├── PRODUCT_SCHEMA.md                  # 제품 표준 스키마 정의 (고정)
│   └── models/                            # 공유 모델 (현재 비어 있음)
```

---

## 3. dbt 모델 네이밍 규칙

dbt는 프로젝트 전체에서 모델명이 유일해야 한다. 테넌트별 모델이 같은 프로젝트에 공존하므로 **테넌트 prefix** 필수:

```
{tenant_id}__{layer}_{entity}.sql
```

| 파일명 | 물리 테이블명 (alias) | 용도 |
|--------|---------------------|------|
| `project_01__stg_cfg_item_master.sql` | `stg_cfg_item_master` | 테넌트별 staging |
| `project_01__mart_item_master.sql` | `mart_item_master` | 제품 표준 output |
| `project_02__stg_cfg_item_master.sql` | `stg_cfg_item_master` | 테넌트별 staging |
| `project_02__mart_item_master.sql` | `mart_item_master` | 제품 표준 output (동일) |

### mart 모델 예시

```sql
-- code_locations/project_01/models/mart/project_01__mart_item_master.sql
{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_01'),
    alias='mart_item_master'                -- ★ 물리 테이블명은 표준
) }}

SELECT
    item_id::VARCHAR          AS item_id,
    item_name::VARCHAR        AS item_name,
    item_type::VARCHAR        AS item_type,
    item_group_id::VARCHAR    AS item_group_id,
    procurement_type::VARCHAR AS procurement_type,
    create_datetime           AS created_at,
    update_datetime           AS updated_at
FROM {{ ref('project_01__stg_cfg_item_master') }}
```

---

## 4. 테넌트별 _sources.yml

각 테넌트의 소스 테이블은 다르므로 `_sources.yml`도 테넌트별로 정의한다.
dbt source name은 `{tenant_id}_raw`로 전역 유일해야 한다.

```yaml
# code_locations/project_01/models/_sources.yml
version: 2
sources:
  - name: project_01_raw
    database: iceberg
    schema: "{{ var('tenant_id', 'project_01') }}_raw"
    tables:
      - name: cfg_item_master
```

---

## 5. Dagster 통합

### TransformDagsterDbtTranslator

테넌트 prefix를 제거하여 asset key를 생성한다:

| dbt 리소스 | Asset Key |
|-----------|-----------|
| source `{tid}_raw.X` | `[tid, "input", X]` |
| model `{tid}__stg_X` | `[tid, "staging", stg_X]` |
| model `{tid}__mart_X` | `[tid, "output", mart_X]` |

### _tenant_factory.py

테넌트의 `models/` 디렉토리를 파일시스템 스캔하여 dbt `@dbt_assets(select=...)` 문자열을 자동 구성한다.
모델명을 수동 나열할 필요 없음.

---

## 6. 자동 생성 (sync_tenants.py)

`scripts/sync_tenants.py`가 `code_locations/*/tenant.yaml`을 스캔하여 3개 파일을 자동 생성:

| 파일 | 내용 |
|------|------|
| `workspace.yaml` | 테넌트별 코드 로케이션 등록 |
| `code_locations/{tid}/__init__.py` | 보일러플레이트 (create_tenant_defs 호출) |
| `dbt_project.yml`의 `model-paths` | 테넌트별 models/ 경로 추가 |

```bash
python scripts/sync_tenants.py          # 동기화
python scripts/sync_tenants.py --check  # CI: 동기화 필요 시 exit 1
```

---

## 7. Iceberg 테이블 결과

```
iceberg.project_01.mart_item_master     ← 제품 표준 스키마
iceberg.project_02.mart_item_master     ← 동일한 컬럼 구조

iceberg.project_01_raw.cfg_item_master  ← 테넌트별 고유 raw 테이블
iceberg.project_02_raw.cfg_item_master  ← 테넌트별 고유 raw 테이블
```

---

## 8. 신규 테넌트 추가 워크플로우

### Step 1: 테넌트 디렉토리 생성 (수동)

```bash
mkdir -p mozart_etl/code_locations/project_03/models/{staging,mart}
```

### Step 2: tenant.yaml 생성 (수동)

```yaml
# code_locations/project_03/tenant.yaml
tenant:
  id: project_03
  source:
    type: mssql
    host: "${PROJECT_03_DB_HOST:localhost}"
    port: "${PROJECT_03_DB_PORT:1433}"
    database: "${PROJECT_03_DB_NAME:erp_db}"
    username: "${PROJECT_03_DB_USER:sa}"
    password: "${PROJECT_03_DB_PASSWORD:pass}"
  storage:
    bucket: "${S3_BUCKET_NAME:warehouse}"
    prefix: "raw/project_03"
  iceberg:
    catalog: iceberg
    schema: project_03
  schedule: "0 */4 * * *"

tables:
  - name: inv_products
    source_schema: dbo
    source_table: INV_PRODUCTS
    primary_key: [product_code]
    mode: full_refresh
```

### Step 3: dbt 모델 생성 (수동)

```yaml
# code_locations/project_03/models/_sources.yml
version: 2
sources:
  - name: project_03_raw
    database: iceberg
    schema: "{{ var('tenant_id', 'project_03') }}_raw"
    tables:
      - name: inv_products
```

```sql
-- code_locations/project_03/models/staging/project_03__stg_inv_products.sql
{{ config(materialized='table', schema=var('tenant_id', 'project_03')) }}

SELECT
    product_code, product_name, category_code,
    group_id, supply_method, reg_date, mod_date
FROM {{ source('project_03_raw', 'inv_products') }}
```

```sql
-- code_locations/project_03/models/mart/project_03__mart_item_master.sql
{{ config(
    materialized='table',
    schema=var('tenant_id', 'project_03'),
    alias='mart_item_master'
) }}

-- 제품 표준 스키마로 매핑 (PRODUCT_SCHEMA.md 참조)
SELECT
    product_code::VARCHAR     AS item_id,
    product_name::VARCHAR     AS item_name,
    category_code::VARCHAR    AS item_type,
    group_id::VARCHAR         AS item_group_id,
    supply_method::VARCHAR    AS procurement_type,
    reg_date                  AS created_at,
    mod_date                  AS updated_at
FROM {{ ref('project_03__stg_inv_products') }}
```

### Step 4: 동기화 + 실행 (자동)

```bash
make dev
# → sync_tenants.py: workspace.yaml + __init__.py + dbt_project.yml 자동 생성
# → dbt parse
# → dagster dev
```

### 변경 파일 요약

| 파일 | 위치 | 방법 |
|------|------|------|
| `code_locations/project_03/tenant.yaml` | code_locations 내부 | 수동 |
| `code_locations/project_03/models/**` | code_locations 내부 | 수동 |
| `code_locations/project_03/__init__.py` | code_locations 내부 | **자동** |
| `workspace.yaml` | 루트 | **자동** |
| `dbt_project.yml` | dbt 프로젝트 | **자동** |

**수동 변경: `code_locations/` 내부만. 외부 수동 변경: 0개.**
