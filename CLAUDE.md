# Mozart ETL - 멀티테넌트 ETL 파이프라인

## 개요

**Mozart ETL**은 Dagster 기반의 멀티테넌트 ETL 플랫폼입니다.
소스 DB에서 추출 → MinIO/S3(Parquet) + Iceberg raw 적재 → dbt staging 변환 → dbt mart 생성의
파이프라인을 테넌트별로 격리하여 운영합니다.

> 현재 등록된 테넌트: `workspace.yaml` 참조
> 지원 DB 커넥터: `mozart_etl/lib/extract/connectors/` 하위 파일 참조

## 아키텍처

```
Source DB  →  MinIO/S3 (Parquet) + Iceberg raw  →  dbt staging 변환  →  dbt mart
 [INPUT]              저장 + 적재                    [STAGING]           [OUTPUT]
```

### 코드 로케이션 패턴 (workspace.yaml)

| 유형 | 모듈 | 역할 |
|------|------|------|
| 테넌트 (N개) | `mozart_etl.code_locations.{tenant_id}` | 테넌트별 input + staging + output 파이프라인 |

### Asset Key 규칙

| 단계 | Asset Key | Group |
|------|-----------|-------|
| INPUT | `[tenant_id, "input", table_name]` | `tenant_id` |
| STAGING | `[tenant_id, "staging", stg_model_name]` | `tenant_id` |
| OUTPUT | `[tenant_id, "output", mart_model_name]` | `tenant_id` |

## 저장소 구조

```
mozart-etl/
├── workspace.yaml                     # ★ 자동 생성 (sync_tenants.py)
├── Makefile                           # dev 워크플로 (sync → dbt-parse → dagster dev)
├── scripts/
│   └── sync_tenants.py                # ★ 자동 생성: workspace.yaml + __init__.py + dbt_project.yml
│
├── mozart_etl/
│   ├── code_locations/                # Dagster 코드 로케이션
│   │   ├── _shared.py                 #   공통 리소스 (S3, Trino) + config 로딩
│   │   ├── _tenant_factory.py         #   테넌트 Definitions 생성 팩토리
│   │   └── {tenant_id}/               # ★ 테넌트별 격리 패키지 (N개)
│   │       ├── __init__.py            #   ★ 자동 생성 (sync_tenants.py)
│   │       ├── tenant.yaml            #   연결정보 + 테이블 정의
│   │       └── models/                #   ★ 테넌트별 dbt 모델
│   │           ├── _sources.yml       #     raw Iceberg source ({tid}_raw)
│   │           ├── staging/           #     변환 모델 ({tid}__stg_{entity}.sql)
│   │           └── mart/              #     mart 모델 ({tid}__mart_{entity}.sql)
│   │
│   ├── lib/                           # 공유 라이브러리 (테넌트 간 공유)
│   │   ├── trino.py                   # TrinoResource (DDL/DQL)
│   │   ├── dbt/translator.py          # TransformDagsterDbtTranslator
│   │   ├── extract/
│   │   │   └── connectors/            # ★ DB 커넥터 팩토리 (확장 가능)
│   │   │       ├── base.py            #   BaseConnector (SQLAlchemy + PyArrow)
│   │   │       └── {db_type}.py       #   DB별 구현체
│   │   └── storage/minio.py           # S3Resource (Parquet R/W)
│   │
│   └── utils/
│       └── environment_helpers.py     # 환경 감지 (LOCAL/BRANCH/PROD)
│
├── mozart_etl_dbt_transform/          # dbt-trino 프로젝트 (공유 인프라)
│   ├── dbt_project.yml                # ★ 자동 생성 (model-paths에 테넌트 경로 포함)
│   ├── profiles.yml                   # Trino 연결 프로필 (dev/prod)
│   ├── macros/generate_schema_name.sql # 커스텀 스키마 (prefix 없음)
│   ├── PRODUCT_SCHEMA.md              # 제품 표준 출력 스키마 정의
│   └── models/                        # 공유 모델 (현재 비어 있음, 테넌트 모델은 code_locations/)
│
└── docs/                              # 설계 문서
    └── tenant-onboarding-architecture.md  # 테넌트 온보딩 아키텍처 설계서
```

## 핵심 파일별 역할

### 코드 로케이션

- **`_tenant_factory.py`**: `create_tenant_defs(Path)` — tenant.yaml 읽어 extract + dbt(staging+output) 에셋, job, schedule 생성. `_get_tenant_dbt_select()` — 파일시스템 스캔으로 dbt select 문자열 구성
- **`_shared.py`**: `get_shared_resources()` — S3Resource, TrinoResource 인스턴스 반환. `load_tenant_config(Path)` — YAML 파싱 + 환경변수 해석. `find_dbt_executable()` — dbt 실행파일 탐색

### 라이브러리

- **`lib/trino.py`**: `TrinoResource` — `get_connection()`, `execute()`, `execute_ddl()`
- **`lib/storage/minio.py`**: `S3Resource` — `write_parquet()`, `read_parquet()`, `list_objects()`
- **`lib/extract/connectors/base.py`**: `BaseConnector` — `extract_table(schema, table, columns, filters, incremental_column)` → PyArrow Table
- **`lib/dbt/translator.py`**: `TransformDagsterDbtTranslator` — source → `[tid, "input", name]`, staging → `[tid, "staging", stg_name]`, mart → `[tid, "output", mart_name]`. 테넌트 prefix 자동 제거

### 자동 생성 스크립트

- **`scripts/sync_tenants.py`**: 테넌트 디렉토리 스캔 → workspace.yaml, __init__.py, dbt_project.yml 자동 생성

### 설정

- **`tenant.yaml`**: 테넌트별 격리 설정. `tenant` (연결정보) + `tables` (추출 대상) 포함. 환경변수는 `${VAR:default}` 패턴
- **`workspace.yaml`**: 자동 생성 — 테넌트별 코드 로케이션 정의
- **`.env`**: 환경변수. tenant.yaml에서 `${VAR_NAME:default}` 패턴으로 참조

## 개발 명령어

```bash
# Makefile 사용 (권장)
make sync          # 테넌트 동기화 (workspace.yaml + __init__.py + dbt_project.yml)
make dbt-parse     # sync + dbt 파싱
make dev           # sync + dbt 파싱 + Dagster 시작
make validate      # sync + dbt 파싱 + Dagster 검증
make check-sync    # CI: 동기화 상태 확인

# 개별 명령어
python scripts/sync_tenants.py                    # 동기화만
cd mozart_etl_dbt_transform && dbt parse && cd ..  # dbt 파싱만
dagster dev -w workspace.yaml                      # Dagster 시작
dagster definitions validate -w workspace.yaml     # 정의 검증
pytest mozart_etl_tests/ -v --tb=short             # 테스트
```

## dbt 모델 네이밍 규칙

dbt는 프로젝트 내 모델명이 전역 고유해야 합니다. 테넌트별 모델은 다음 규칙을 따릅니다:

| 항목 | 규칙 | 예시 |
|------|------|------|
| dbt 모델 파일명 | `{tid}__{layer}_{entity}.sql` | `project_01__stg_cfg_item_master.sql` |
| dbt 모델명 (내부) | `{tid}__{layer}_{entity}` | `project_01__stg_cfg_item_master` |
| Iceberg 물리 테이블명 | `alias` config 사용 | `mart_item_master` (mart만) |
| Dagster Asset Key | prefix 제거 후 매핑 | `[project_01, "staging", stg_cfg_item_master]` |
| dbt source 이름 | `{tid}_raw` | `project_01_raw` |

## 확장 규칙

### 신규 테넌트 추가

1. `code_locations/{tenant_id}/tenant.yaml` 생성 (연결정보 + 테이블 정의)
2. `code_locations/{tenant_id}/models/_sources.yml` 생성 (source: `{tid}_raw`)
3. `code_locations/{tenant_id}/models/staging/{tid}__stg_{entity}.sql` 생성
4. `code_locations/{tenant_id}/models/mart/{tid}__mart_{entity}.sql` 생성 (alias 필수)
5. `make sync` 실행 → workspace.yaml + __init__.py + dbt_project.yml 자동 생성

> **주의**: workspace.yaml, __init__.py, dbt_project.yml은 직접 편집하지 말 것 (sync_tenants.py가 관리)

### 신규 테이블 추가 (기존 테넌트)

1. `tenant.yaml`의 `tables` 목록에 추가
2. `_sources.yml`에 테이블 추가
3. `models/staging/{tid}__stg_{table}.sql` 추가
4. `models/mart/{tid}__mart_{table}.sql` 추가 (alias + PRODUCT_SCHEMA.md 참조)
5. `make dbt-parse` 실행

### 신규 DB 커넥터 추가

1. `lib/extract/connectors/{db_type}.py` — `BaseConnector` 상속 구현
2. `lib/extract/connectors/__init__.py` — `create_connector()`에 타입 등록
3. `pyproject.toml` — 드라이버 패키지 추가

## 패턴 및 규칙

### 환경변수 해석
tenant.yaml에서 `${VAR_NAME:default}` 패턴 사용. `_shared.py`의 `_resolve_env_vars()`가 처리.

### 테넌트 필터링
`tenant_filter` + `params`로 멀티테넌트 DB에서 특정 테넌트 데이터만 추출:
```yaml
params:
  project_id: "UUID-..."
tables:
  - tenant_filter: project_id   # WHERE project_id = :filter_0
```

### dbt DAG 연결
dbt source의 asset_key가 Dagster INPUT 에셋과 연결:
- source `{tid}_raw.{table}` → `[tenant_id, "input", table]`
- staging model `{tid}__stg_{entity}` → `[tenant_id, "staging", stg_{entity}]`
- mart model `{tid}__mart_{entity}` → `[tenant_id, "output", mart_{entity}]`

### 자동화 조건
- INPUT: `AutomationCondition.on_cron(schedule)` — 테넌트 cron에 따라 실행
- STAGING/OUTPUT: `AutomationCondition.eager()` — 의존성 완료 시 즉시 실행

### 제품 표준 스키마
모든 테넌트의 mart 모델은 `PRODUCT_SCHEMA.md`에 정의된 동일한 컬럼 구조를 준수해야 합니다.
테넌트별 소스 테이블은 다를 수 있지만, mart 출력은 표준화됩니다.

## Docker 인프라

```bash
docker compose up -d   # docker-compose.yml 참조
docker compose down
```

## 기술 스택

Dagster | dbt-trino | Trino | Iceberg | MinIO | PyArrow | SQLAlchemy — 버전은 `pyproject.toml` 참조
