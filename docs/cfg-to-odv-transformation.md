# CFG → ODV 테이블 변환 가이드

## 개요

이 가이드는 project_01의 CFG% 테이블 데이터를 ODV% 테이블로 변환하는 파이프라인에 대해 설명합니다.

## 아키텍처

```
┌─────────────────┐
│  CFG 테이블     │ PostgreSQL (소스)
│  cfg_*          │
└────────┬────────┘
         │ [INPUT] Dagster extract asset
         ▼
┌─────────────────┐
│  Iceberg Raw    │ Trino/Iceberg
│  project_01_raw │
└────────┬────────┘
         │ [STAGING] dbt transformation
         ▼
┌─────────────────┐
│  ODV 변환 중간  │ Iceberg staging
│  stg_cfg_to_odv │
└────────┬────────┘
         │ [OUTPUT] dbt mart
         ▼
┌─────────────────┐
│  ODV Mart       │ Iceberg mart
│  mart_odv_*     │
└─────────────────┘
```

## 구현된 테이블 (Phase 1)

### 1. cfg_item_master → odv_item_master

**변환 규칙:**
- `partition_key`: `{project_id}_{plan_ver}` 생성
- `plan_ver`: 변수로 지정 (기본값: `V1.0`)
- `item_type`: `_ODV` 접미사 추가
- `item_name`: `[변환됨]` 접두사 추가
- `create_datetime`, `update_datetime`: 현재 시간으로 교체

**파일:**
- Staging: `models/staging/project_01__stg_cfg_to_odv_item_master.sql`
- Mart: `models/mart/project_01__mart_odv_item_master.sql`

### 2. cfg_demand → odv_demand

**변환 규칙:**
- `partition_key`: `{project_id}_{plan_ver}` 생성
- `plan_ver`: 변수로 지정 (기본값: `V1.0`)
- `demand_qty`: 10% 증가 (`* 1.1`)
- `demand_type`: `_PLANNED` 접미사 추가

**파일:**
- Staging: `models/staging/project_01__stg_cfg_to_odv_demand.sql`
- Mart: `models/mart/project_01__mart_odv_demand.sql`

### 3. cfg_bom_master → odv_bom_master

**변환 규칙:**
- `partition_key`: `{project_id}_{plan_ver}` 생성
- `plan_ver`: 변수로 지정 (기본값: `V1.0`)
- `description`: `[계획용]` 접두사 추가
- `create_datetime`, `update_datetime`: 현재 시간으로 교체

**파일:**
- Staging: `models/staging/project_01__stg_cfg_to_odv_bom_master.sql`
- Mart: `models/mart/project_01__mart_odv_bom_master.sql`

## 실행 방법

### 1. 전체 파이프라인 실행 (Dagster UI)

1. Dagster UI 접속: http://127.0.0.1:3000
2. **Assets** 메뉴 클릭
3. `project_01` 그룹 선택
4. 실행할 asset 선택:
   - INPUT: `cfg_item_master`, `cfg_demand`, `cfg_bom_master`
   - STAGING: `stg_cfg_to_odv_*`
   - OUTPUT: `mart_odv_*`
5. **Materialize selected** 클릭

### 2. dbt 직접 실행

```bash
cd mozart_etl_dbt_transform

# 기본 plan_ver (V1.0) 사용
dbt build --select project_01__stg_cfg_to_odv_*

# plan_ver 커스텀 지정
dbt build --select project_01__stg_cfg_to_odv_* \
  --vars '{"plan_ver": "2024-Q1-SCENARIO-A"}'

# 특정 모델만 실행
dbt run --select project_01__stg_cfg_to_odv_item_master
dbt run --select project_01__mart_odv_item_master
```

### 3. Dagster에서 config로 plan_ver 지정

Dagster UI에서 asset materialize 시 config 입력:

```yaml
ops:
  project_01__staging:
    config:
      dbt_vars:
        plan_ver: "2024-Q1-SCENARIO-A"
```

## 변환 로직 커스터마이징

### plan_ver 값 지정

`plan_ver`는 ODV 테이블의 계획 버전을 구분하는 중요한 키입니다.

**지정 방법:**
- dbt 변수: `--vars '{"plan_ver": "값"}'`
- Dagster config: `dbt_vars: {plan_ver: "값"}`
- 기본값: `V1.0`

**권장 네이밍:**
- 시나리오 기반: `SCENARIO-001`, `SCENARIO-BASELINE`
- 날짜 기반: `2024-Q1`, `2024-02-PLAN`
- 버전 기반: `V1.0`, `V2.0`, `V2.1`

### 샘플 변환 규칙 수정

각 staging 모델 파일(`project_01__stg_cfg_to_odv_*.sql`)을 편집하여 변환 규칙을 수정할 수 있습니다.

**예시: demand_qty 증가율 변경**

```sql
-- 현재 (10% 증가)
demand_qty * 1.1 AS demand_qty,

-- 변경 (20% 증가)
demand_qty * 1.2 AS demand_qty,

-- 변경 (고정값 적용)
100.0 AS demand_qty,
```

## 데이터 검증

### 1. Trino에서 조회

```sql
-- Staging 테이블 확인
SELECT * FROM iceberg.project_01.project_01__stg_cfg_to_odv_item_master
LIMIT 10;

-- Mart 테이블 확인
SELECT * FROM iceberg.project_01.mart_odv_item_master
LIMIT 10;

-- plan_ver별 데이터 개수 확인
SELECT plan_ver, COUNT(*)
FROM iceberg.project_01.mart_odv_item_master
GROUP BY plan_ver;
```

### 2. Dagster UI에서 확인

1. Assets 메뉴에서 해당 asset 클릭
2. **Materialize info** 탭에서 실행 로그 확인
3. **Metadata** 탭에서 행 개수, 컬럼 정보 확인

## 확장 계획

### Phase 2: 전체 테이블 확장

현재 3개 테이블 구현 완료. 향후 69개 ODV 테이블로 확장 예정.

**자동 생성 스크립트:**
```python
# scripts/generate_cfg_to_odv_models.py (예정)
# - 메타데이터 기반 dbt 모델 자동 생성
# - 컬럼 매핑 규칙 정의
# - 템플릿 기반 SQL 생성
```

### Phase 3: 실제 ODV 테이블 적재

현재는 Iceberg mart 테이블까지만 생성. 실제 PostgreSQL ODV 테이블 INSERT는 별도 구현 필요.

**옵션:**
1. Trino → PostgreSQL INSERT 쿼리 실행
2. Iceberg → Parquet → PostgreSQL COPY
3. dbt post-hook 활용

## 파일 구조

```
mozart_etl/code_locations/project_01/
├── tenant.yaml                    # CFG 테이블 INPUT 정의 (3개)
├── models/
│   ├── _sources.yml               # source: project_01_raw
│   ├── staging/
│   │   ├── project_01__stg_cfg_item_master.sql           # 기존 (1:1 복사)
│   │   ├── project_01__stg_cfg_to_odv_item_master.sql    # ★ 신규 (변환)
│   │   ├── project_01__stg_cfg_to_odv_demand.sql         # ★ 신규
│   │   └── project_01__stg_cfg_to_odv_bom_master.sql     # ★ 신규
│   └── mart/
│       ├── project_01__mart_item_master.sql              # 기존
│       ├── project_01__mart_odv_item_master.sql          # ★ 신규
│       ├── project_01__mart_odv_demand.sql               # ★ 신규
│       └── project_01__mart_odv_bom_master.sql           # ★ 신규
```

## 트러블슈팅

### 문제: dbt 모델이 인식되지 않음

**원인:** dbt_project.yml의 model-paths 설정

**해결:**
```bash
# 테넌트 동기화 실행
make sync

# dbt 파싱 재실행
cd mozart_etl_dbt_transform && dbt parse
```

### 문제: partition_key 중복

**원인:** 동일한 plan_ver로 여러 번 실행

**해결:**
- 다른 plan_ver 값 사용
- 기존 데이터 삭제 후 재실행

```sql
-- 기존 데이터 삭제
DELETE FROM iceberg.project_01.mart_odv_item_master
WHERE plan_ver = 'V1.0';
```

### 문제: 컬럼 타입 불일치

**원인:** CFG 테이블과 ODV 테이블 스키마 차이

**해결:** staging SQL에서 명시적 타입 캐스팅

```sql
-- 예시
CAST(item_priority AS INTEGER) AS item_priority
```

## 참고 자료

- [Mozart ETL 아키텍처](../CLAUDE.md)
- [테넌트 온보딩 가이드](./tenant-onboarding-architecture.md)
- [dbt 공식 문서](https://docs.getdbt.com/)
- [Trino 공식 문서](https://trino.io/docs/current/)
