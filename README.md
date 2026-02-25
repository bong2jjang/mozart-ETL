# Mozart ETL

Dagster 기반 멀티테넌트 ETL 파이프라인 플랫폼.

소스 DB에서 데이터를 추출하여 MinIO/S3(Parquet)에 저장하고,
Trino/Iceberg raw 테이블로 적재한 뒤, dbt SQL로 변환하고,
dbt mart로 최종 테이블을 생성합니다. 모든 단계가 테넌트별 독립 실행됩니다.

> 현재 등록된 테넌트: `workspace.yaml` 참조
> 지원 DB 커넥터: `mozart_etl/lib/extract/connectors/` 참조

## 아키텍처

```
┌──────────┐    ┌───────────┐    ┌───────────────┐    ┌──────────┐
│1.Source DB │───▶│2.MinIO/S3  │───▶│3.Trino/Iceberg │───▶│4.dbt mart │
│(per tenant)│    │(Parquet)  │    │  (staging)    │    │(per tenant)│
└──────────┘    └───────────┘    └───────────────┘    └──────────┘
   [INPUT]       + Iceberg raw      [STAGING]            [OUTPUT]
  extract+load   {tid}_raw.*       dbt SQL 변환          dbt mart
```

### Dagster UI 구조

```
Code Locations:                        # workspace.yaml (auto-generated)
├── {tenant_id}                        # 테넌트별 독립 프로세스 (N개)
│   ├── input/{table_name}             #   RDB → S3 Parquet + Iceberg raw table
│   ├── staging/stg_{table_name}       #   dbt staging 변환 → Iceberg table
│   └── output/mart_{table_name}       #   dbt mart → Iceberg table (per-tenant)
```

## 프로젝트 구조

```
mozart-etl/
├── workspace.yaml                     # ★ 자동 생성 (sync_tenants.py)
├── Makefile                           # dev 워크플로 (sync → parse → dev)
├── docker-compose.yml                 # MinIO + Trino + Iceberg REST + PostgreSQL
├── pyproject.toml                     # 패키지 설정
├── .env                               # 환경변수 (DB 접속, S3, Trino 등)
│
├── scripts/
│   └── sync_tenants.py                # ★ 자동 생성: workspace.yaml + __init__.py + dbt_project.yml
│
├── mozart_etl/                        # 메인 Python 패키지
│   ├── code_locations/                # Dagster 코드 로케이션
│   │   ├── _shared.py                 #   공통 리소스 + 설정 로딩 + dbt 유틸
│   │   ├── _tenant_factory.py         #   테넌트 Definitions 팩토리
│   │   └── {tenant_id}/              #   ★ 테넌트별 격리 패키지 (N개)
│   │       ├── __init__.py            #     ★ 자동 생성 (sync_tenants.py)
│   │       ├── tenant.yaml            #     연결정보 + 테이블 정의
│   │       └── models/                #     ★ 테넌트별 dbt 모델
│   │           ├── _sources.yml       #       raw Iceberg source ({tid}_raw)
│   │           ├── staging/           #       변환 모델 ({tid}__stg_*.sql)
│   │           └── mart/              #       mart 모델 ({tid}__mart_*.sql)
│   ├── lib/                           # 공유 라이브러리
│   │   ├── trino.py                   #   TrinoResource (DDL/DQL)
│   │   ├── dbt/translator.py          #   TransformDagsterDbtTranslator
│   │   ├── extract/connectors/        #   ★ DB 커넥터 (확장 가능)
│   │   │   ├── base.py                #     BaseConnector (SQLAlchemy)
│   │   │   └── {db_type}.py           #     DB별 구현체
│   │   └── storage/minio.py           #   S3Resource (Parquet R/W)
│   └── utils/
│       └── environment_helpers.py     # 환경 감지 (LOCAL/BRANCH/PROD)
│
├── mozart_etl_dbt_transform/          # dbt-trino 프로젝트 (공유 인프라)
│   ├── dbt_project.yml                #   ★ 자동 생성 (model-paths에 테넌트 경로)
│   ├── profiles.yml                   #   Trino 연결 (dev/prod)
│   ├── macros/
│   │   └── generate_schema_name.sql   #   커스텀 스키마 (prefix 없음)
│   ├── PRODUCT_SCHEMA.md              #   제품 표준 출력 스키마 정의
│   └── models/                        #   공유 모델 (테넌트 모델은 code_locations/)
│
├── docs/                              # 설계 문서
│   └── tenant-onboarding-architecture.md
│
└── docker/                            # Docker 설정
    ├── trino/catalog/                 #   Iceberg 카탈로그 설정
    └── postgres/init.sql              #   샘플 DB 초기화
```

## 로컬 개발

### 사전 요구사항

- Python 3.12+
- Docker & Docker Compose
- [uv](https://docs.astral.sh/uv/) (패키지 매니저)

### 시작하기

```bash
# 1. Python 환경 설정
uv venv --python 3.12
uv sync

# 2. Docker 인프라 시작
docker compose up -d

# 3. 환경변수 설정
cp .env.example .env   # 또는 기존 .env 사용

# 4. Makefile 사용 (권장)
make dev               # sync → dbt-parse → dagster dev (한 번에)
```

### Makefile 명령어

```bash
make sync          # 테넌트 동기화 (workspace.yaml + __init__.py + dbt_project.yml)
make dbt-parse     # sync + dbt 파싱
make dev           # sync + dbt 파싱 + Dagster 시작
make validate      # sync + dbt 파싱 + 정의 검증
make check-sync    # CI: 동기화 상태 확인
```

### VS Code 실행

`.vscode/launch.json`에 미리 정의된 설정이 있습니다:

- **Dagster Dev (멀티 로케이션)**: `dagster dev -w workspace.yaml`
- **Dagster Validate (정의 검증)**: `dagster definitions validate -w workspace.yaml`
- **dbt Parse**: dbt manifest 생성
- **Pytest**: 테스트 실행

### 검증

```bash
# 전체 workspace 검증
make validate

# 개별 코드 로케이션 검증
dagster definitions validate -m mozart_etl.code_locations.{tenant_id}
```

## 핵심 개념

### 테넌트 격리

각 테넌트는 완전히 격리된 패키지로 운영됩니다:

- **독립 설정**: `tenant.yaml`에 연결정보 + 테이블 정의 포함
- **독립 프로세스**: workspace.yaml에서 별도 code location으로 실행
- **독립 dbt 모델**: 테넌트별 staging/mart 모델 (소스 테이블이 다를 수 있음)
- **표준 출력**: 모든 테넌트의 mart 모델은 동일한 제품 표준 스키마 준수 (`PRODUCT_SCHEMA.md`)
- **교차 영향 없음**: 한 테넌트 수정이 다른 테넌트에 영향 없음

```yaml
# code_locations/{tenant_id}/tenant.yaml
tenant:
  id: {tenant_id}
  source:
    type: postgresql          # 지원 타입은 lib/extract/connectors/ 참조
    host: "${TENANT_DB_HOST:localhost}"
    port: "${TENANT_DB_PORT:5432}"
    database: "${TENANT_DB_NAME:mydb}"
    username: "${TENANT_DB_USER:user}"
    password: "${TENANT_DB_PASSWORD:pass}"
  params:                     # 테넌트 필터링용 파라미터 (선택)
    project_id: "UUID-..."
  storage:
    bucket: "${S3_BUCKET_NAME:warehouse}"
    prefix: "raw/{tenant_id}"
  iceberg:
    catalog: iceberg
    schema: {tenant_id}
  schedule: "0 */2 * * *"    # 테넌트별 스케줄

tables:
  - name: {table_name}
    source_schema: public
    source_table: {table_name}
    primary_key: [col1, col2]
    columns: [col1, col2, ...]        # 추출 대상 컬럼 (선택)
    tenant_filter: project_id         # params의 키로 WHERE 필터 (선택)
    incremental_column: updated_at    # 증분 추출 기준 (선택)
    mode: incremental                 # incremental | full_refresh
```

### 파이프라인 흐름

| 단계 | Asset Key | 설명 |
|------|-----------|------|
| **INPUT** | `[tenant_id, "input", table]` | 소스 DB에서 추출 → S3 Parquet + Iceberg raw 테이블 적재 |
| **STAGING** | `[tenant_id, "staging", stg_model]` | dbt staging 변환 → Trino/Iceberg 변환 테이블 생성 |
| **OUTPUT** | `[tenant_id, "output", mart_model]` | dbt mart → Trino/Iceberg 최종 테이블 (per-tenant, 표준 스키마) |

### Iceberg 스키마 규칙

| 레이어 | 스키마 | 예시 |
|--------|--------|------|
| INPUT (raw) | `iceberg.{tenant_id}_raw` | `iceberg.project_01_raw.cfg_item_master` |
| STAGING | `iceberg.{tenant_id}` | `iceberg.project_01.stg_cfg_item_master` |
| OUTPUT | `iceberg.{tenant_id}` | `iceberg.project_01.mart_item_master` |

### dbt 모델 네이밍

테넌트별 dbt 모델은 전역 고유성을 위해 prefix 규칙을 따릅니다:

| 항목 | 규칙 | 예시 |
|------|------|------|
| 파일명 | `{tid}__{layer}_{entity}.sql` | `project_01__stg_cfg_item_master.sql` |
| dbt 모델명 | `{tid}__{layer}_{entity}` | `project_01__stg_cfg_item_master` |
| 물리 테이블명 | `alias` config (mart만) | `mart_item_master` |
| Asset Key | prefix 제거 후 매핑 | `[project_01, "staging", stg_cfg_item_master]` |
| dbt source | `{tid}_raw` | `project_01_raw` |

### 리소스

| 리소스 | 용도 |
|--------|------|
| `S3Resource` | MinIO/S3 호환 스토리지 (Parquet 읽기/쓰기) |
| `TrinoResource` | Trino 쿼리 엔진 (DDL/DQL 실행) |
| `DbtCliResource` | dbt-trino CLI 실행 (staging + output) |

## 신규 테넌트 추가

새 테넌트를 추가하려면 `code_locations/` 하위만 수정합니다:

| 단계 | 파일 | 설명 |
|------|------|------|
| 1 | `code_locations/{tid}/tenant.yaml` | 연결정보 + 테이블 정의 생성 |
| 2 | `code_locations/{tid}/models/_sources.yml` | dbt source 정의 (`{tid}_raw`) |
| 3 | `code_locations/{tid}/models/staging/{tid}__stg_*.sql` | staging 모델 |
| 4 | `code_locations/{tid}/models/mart/{tid}__mart_*.sql` | mart 모델 (`alias` + 표준 스키마) |
| 5 | `make sync` 실행 | workspace.yaml + __init__.py + dbt_project.yml 자동 생성 |

> `workspace.yaml`, `__init__.py`, `dbt_project.yml`은 직접 편집하지 마세요. `scripts/sync_tenants.py`가 자동 관리합니다.

## 신규 테이블 추가

기존 테넌트에 새 테이블을 추가:

| 단계 | 파일 | 설명 |
|------|------|------|
| 1 | `tenant.yaml`의 `tables` | 테이블 정의 추가 |
| 2 | `models/_sources.yml` | raw source 테이블 추가 |
| 3 | `models/staging/{tid}__stg_{table}.sql` | staging 모델 추가 |
| 4 | `models/mart/{tid}__mart_{table}.sql` | mart 모델 추가 (`PRODUCT_SCHEMA.md` 참조) |
| 5 | `make dbt-parse` 실행 | dbt manifest 갱신 |

## 신규 DB 커넥터 추가

새 DB 타입을 지원하려면:

1. `mozart_etl/lib/extract/connectors/{db_type}.py` — `BaseConnector` 상속 구현
2. `mozart_etl/lib/extract/connectors/__init__.py` — `create_connector()`에 타입 등록
3. `pyproject.toml` — 필요한 드라이버 패키지 추가

## Docker 인프라

| 서비스 | 포트 | 용도 |
|--------|------|------|
| MinIO | 9000 (API), 9001 (Console) | S3 호환 오브젝트 스토리지 |
| Iceberg REST | 8181 | Iceberg 카탈로그 서비스 |
| Trino | 8080 | 분산 SQL 쿼리 엔진 |
| PostgreSQL | 5433 | 샘플 테넌트 소스 DB |

```bash
docker compose up -d    # 전체 시작
docker compose down     # 전체 중지
```

## 기술 스택

| 컴포넌트 | 용도 | 버전 |
|----------|------|------|
| Dagster | 데이터 오케스트레이션 | `pyproject.toml` 참조 |
| dbt-core + dbt-trino | SQL 변환 | `pyproject.toml` 참조 |
| Trino | 분산 SQL 쿼리 엔진 | latest |
| Iceberg | 오픈 테이블 포맷 | latest |
| MinIO | S3 호환 스토리지 | latest |
| PyArrow | 컬럼나 데이터 처리 | `pyproject.toml` 참조 |
| SQLAlchemy | DB 커넥터 추상화 | `pyproject.toml` 참조 |
