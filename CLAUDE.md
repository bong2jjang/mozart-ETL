# Mozart ETL - 멀티테넌트 ETL 파이프라인

## 개요

**Mozart ETL**은 Dagster 기반의 멀티테넌트 ETL 플랫폼입니다.
소스 DB에서 추출 → MinIO/S3(Parquet) + Iceberg raw 적재 → dbt SQL 변환 → dbt mart 생성의
파이프라인을 테넌트별로 격리하여 운영합니다.

> 현재 등록된 테넌트: `workspace.yaml` 참조
> 지원 DB 커넥터: `mozart_etl/lib/extract/connectors/` 하위 파일 참조

## 아키텍처

```
Source DB  →  MinIO/S3 (Parquet) + Iceberg raw  →  dbt SQL 변환  →  dbt mart
 [INPUT]              저장 + 적재                   [TRANSFORM]      [OUTPUT]
```

### 코드 로케이션 패턴 (workspace.yaml)

| 유형 | 모듈 | 역할 |
|------|------|------|
| 테넌트 (N개) | `mozart_etl.code_locations.{tenant_id}` | 테넌트별 input + transform + output 파이프라인 |

### Asset Key 규칙

| 단계 | Asset Key | Group |
|------|-----------|-------|
| INPUT | `[tenant_id, "input", table_name]` | `tenant_id` |
| TRANSFORM | `[tenant_id, "transform", model_name]` | `tenant_id` |
| OUTPUT | `[tenant_id, "output", model_name]` | `tenant_id` |

## 저장소 구조

```
mozart_etl/
├── code_locations/                # Dagster 코드 로케이션
│   ├── _shared.py                 # 공통 리소스 (S3, Trino) + config 로딩
│   ├── _tenant_factory.py         # 테넌트 Definitions 생성 팩토리
│   └── {tenant_id}/               # ★ 테넌트별 격리 패키지 (N개)
│       ├── __init__.py            #   엔트리: create_tenant_defs(tenant.yaml)
│       └── tenant.yaml            #   연결정보 + 테이블 정의
│
├── lib/                           # 공유 라이브러리 (테넌트 간 공유)
│   ├── trino.py                   # TrinoResource (DDL/DQL)
│   ├── dbt/translator.py          # TransformDagsterDbtTranslator
│   ├── extract/
│   │   └── connectors/            # ★ DB 커넥터 팩토리 (확장 가능)
│   │       ├── base.py            #   BaseConnector (SQLAlchemy + PyArrow)
│   │       └── {db_type}.py       #   DB별 구현체
│   └── storage/minio.py           # S3Resource (Parquet R/W)
│
└── utils/
    └── environment_helpers.py     # 환경 감지 (LOCAL/BRANCH/PROD)

mozart_etl_dbt_transform/          # dbt-trino 프로젝트 (TRANSFORM + OUTPUT)
├── dbt_project.yml                # 변환/출력 모델 설정
├── profiles.yml                   # Trino 연결 프로필 (dev/prod)
├── macros/generate_schema_name.sql # 커스텀 스키마 (prefix 없음)
└── models/
    ├── _sources.yml               # raw Iceberg source ({{ var('tenant_id') }}_raw)
    ├── transform/                 # 변환 모델 (raw → transformed)
    └── output/                    # mart 모델 (transformed → final)
```

## 핵심 파일별 역할

### 코드 로케이션

- **`_tenant_factory.py`**: `create_tenant_defs(Path)` — tenant.yaml을 읽어 extract + dbt(transform+output) 에셋, job, schedule을 생성
- **`_shared.py`**: `get_shared_resources()` — S3Resource, TrinoResource 인스턴스 반환. `load_tenant_config(Path)` — YAML 파싱 + 환경변수 해석. `find_dbt_executable()` — dbt 실행파일 탐색

### 라이브러리

- **`lib/trino.py`**: `TrinoResource` — `get_connection()`, `execute()`, `execute_ddl()`
- **`lib/storage/minio.py`**: `S3Resource` — `write_parquet()`, `read_parquet()`, `list_objects()`
- **`lib/extract/connectors/base.py`**: `BaseConnector` — `extract_table(schema, table, columns, filters, incremental_column)` → PyArrow Table
- **`lib/dbt/translator.py`**: `TransformDagsterDbtTranslator` — source는 `[tid, "input", name]`, transform은 `[tid, "transform", name]`, output은 `[tid, "output", name]`으로 매핑

### 설정

- **`tenant.yaml`**: 테넌트별 격리 설정. `tenant` (연결정보) + `tables` (추출 대상) 포함. 환경변수는 `${VAR:default}` 패턴
- **`workspace.yaml`**: 테넌트별 코드 로케이션 정의
- **`.env`**: 환경변수. tenant.yaml에서 `${VAR_NAME:default}` 패턴으로 참조

## 개발 명령어

```bash
# Dagster 시작 (멀티 코드 로케이션)
dagster dev -w workspace.yaml

# 정의 검증
dagster definitions validate -w workspace.yaml

# dbt 파싱
cd mozart_etl_dbt_transform && dbt parse && cd ..

# 테스트
pytest mozart_etl_tests/ -v --tb=short
```

## 확장 규칙

### 신규 테넌트 추가

1. `code_locations/{tenant_id}/tenant.yaml` 생성
2. `code_locations/{tenant_id}/__init__.py` 생성 (3줄 보일러플레이트)
3. `workspace.yaml`에 코드 로케이션 추가

### 신규 테이블 추가

1. `tenant.yaml`의 `tables` 목록에 추가
2. `mozart_etl_dbt_transform/models/transform/{table}.sql` 추가
3. `mozart_etl_dbt_transform/models/output/mart_{table}.sql` 추가
4. `mozart_etl_dbt_transform/models/_sources.yml`에 테이블 추가

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
- source `raw.{table}` → `[tenant_id, "input", table]`
- transform model → `[tenant_id, "transform", model]`
- output model → `[tenant_id, "output", model]`

### 자동화 조건
- INPUT: `AutomationCondition.on_cron(schedule)` — 테넌트 cron에 따라 실행
- TRANSFORM/OUTPUT: `AutomationCondition.eager()` — 의존성 완료 시 즉시 실행

## Docker 인프라

```bash
docker compose up -d   # docker-compose.yml 참조
docker compose down
```

## 기술 스택

Dagster | dbt-trino | Trino | Iceberg | MinIO | PyArrow | SQLAlchemy — 버전은 `pyproject.toml` 참조
