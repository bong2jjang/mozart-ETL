# Mozart ETL

Dagster 기반 멀티테넌트 ETL 파이프라인 플랫폼.

소스 DB에서 데이터를 추출하여 MinIO/S3(Parquet)에 저장하고,
Trino/Iceberg로 적재한 뒤, dbt mart로 테넌트 통합 테이블을 생성합니다.

> 현재 등록된 테넌트: `workspace.yaml` 참조
> 지원 DB 커넥터: `mozart_etl/lib/extract/connectors/` 참조

## 아키텍처

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Source DB   │     │   MinIO / S3     │     │  Trino/Iceberg   │     │   dbt mart   │
│  (per tenant)│────▶│  (Parquet)       │────▶│  (per tenant)    │────▶│  (UNION ALL) │
└──────────────┘     └──────────────────┘     └──────────────────┘     └──────────────┘
    [INPUT]              raw/{tid}/              iceberg.{tid}.          output/
   extract_table       {table}/data.parquet     {table}             mart_{table}_all
```

### Dagster UI 구조

```
Code Locations:                        # workspace.yaml에 정의
├── {tenant_id}                        # 테넌트별 독립 프로세스 (N개)
│   ├── input/{table_name}             #   RDB → Parquet
│   └── transform/{table_name}         #   Parquet → Iceberg
└── output                             # dbt mart (테넌트 통합, 1개)
    └── mart_{table_name}_all          #   UNION ALL across tenants
```

## 프로젝트 구조

```
dagster-open-platform/
├── workspace.yaml                     # Dagster 멀티 코드 로케이션 정의
├── docker-compose.yml                 # MinIO + Trino + Iceberg REST + PostgreSQL
├── pyproject.toml                     # 패키지 설정
├── .env                               # 환경변수 (DB 접속, S3, Trino 등)
│
├── mozart_etl/                        # 메인 Python 패키지
│   ├── code_locations/                # Dagster 코드 로케이션
│   │   ├── _shared.py                 #   공통 리소스 + 설정 로딩 유틸
│   │   ├── _tenant_factory.py         #   테넌트 Definitions 팩토리
│   │   ├── {tenant_id}/              #   ★ 테넌트별 격리 패키지 (N개)
│   │   │   ├── __init__.py            #     엔트리: create_tenant_defs(tenant.yaml)
│   │   │   └── tenant.yaml            #     연결정보 + 테이블 정의
│   │   └── output/                    #   dbt mart 코드 로케이션
│   │       └── __init__.py
│   ├── lib/                           # 공유 라이브러리
│   │   ├── trino.py                   #   TrinoResource (DDL/DQL)
│   │   ├── dbt/translator.py          #   dbt → Dagster 에셋 변환기
│   │   ├── extract/connectors/        #   ★ DB 커넥터 (확장 가능)
│   │   │   ├── base.py                #     BaseConnector (SQLAlchemy)
│   │   │   └── {db_type}.py           #     DB별 구현체
│   │   └── storage/minio.py           #   S3Resource (Parquet R/W)
│   └── utils/
│       └── environment_helpers.py     # 환경 감지 (LOCAL/BRANCH/PROD)
│
├── mozart_etl_dbt/                    # dbt-trino 프로젝트 (OUTPUT 레이어)
│   ├── dbt_project.yml                #   vars.tenants: 테넌트 목록
│   ├── profiles.yml                   #   Trino 연결 (dev/prod)
│   ├── models/
│   │   ├── staging/sources.yml        #   테넌트별 Iceberg 소스 선언
│   │   └── mart/                      #   통합 mart 모델
│   └── macros/
│       └── union_tenants.sql          #   테넌트 UNION ALL 매크로
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

# 4. dbt 의존성 설치 및 파싱
cd mozart_etl_dbt && dbt deps && dbt parse && cd ..

# 5. Dagster 시작 (멀티 코드 로케이션)
dagster dev -w workspace.yaml
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
dagster definitions validate -w workspace.yaml

# 개별 코드 로케이션 검증
dagster definitions validate -m mozart_etl.code_locations.{tenant_id}
dagster definitions validate -m mozart_etl.code_locations.output
```

## 핵심 개념

### 테넌트 격리

각 테넌트는 완전히 격리된 패키지로 운영됩니다:

- **독립 설정**: `tenant.yaml`에 연결정보 + 테이블 정의 포함
- **독립 프로세스**: workspace.yaml에서 별도 code location으로 실행
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
    bucket: "${S3_BUCKET_NAME:data-lake}"
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
| **INPUT** | `[tenant_id, "input", table]` | 소스 DB에서 추출 → S3 Parquet 저장 |
| **TRANSFORM** | `[tenant_id, "transform", table]` | S3 Parquet → Trino/Iceberg 테이블 생성 |
| **OUTPUT** | `["output", model]` | dbt가 테넌트별 Iceberg를 UNION ALL → mart 테이블 |

### 리소스

| 리소스 | 용도 |
|--------|------|
| `S3Resource` | MinIO/S3 호환 스토리지 (Parquet 읽기/쓰기) |
| `TrinoResource` | Trino 쿼리 엔진 (DDL/DQL 실행) |
| `DbtCliResource` | dbt-trino CLI 실행 |

### dbt 통합

dbt는 **OUTPUT 레이어 전용**으로 사용됩니다:

- `sources.yml`: 테넌트별 Iceberg 테이블을 source로 선언 (`meta.dagster.asset_key`로 DAG 연결)
- `union_tenants` 매크로: `dbt_project.yml`의 `vars.tenants` 목록을 순회하며 자동 UNION ALL
- mart 모델: 모든 테넌트 데이터를 통합한 최종 테이블

## 신규 테넌트 추가

새 테넌트를 추가하려면 다음 파일만 수정합니다:

| 파일 | 작업 |
|------|------|
| `mozart_etl/code_locations/{tenant_id}/tenant.yaml` | 테넌트 설정 + 테이블 정의 생성 |
| `mozart_etl/code_locations/{tenant_id}/__init__.py` | 엔트리포인트 생성 (아래 3줄) |
| `workspace.yaml` | 코드 로케이션 등록 |
| `mozart_etl_dbt/models/staging/sources.yml` | dbt source 추가 |
| `mozart_etl_dbt/dbt_project.yml` | `vars.tenants` 목록에 추가 |

```python
# code_locations/{tenant_id}/__init__.py  (모든 테넌트 동일)
from pathlib import Path
from mozart_etl.code_locations._tenant_factory import create_tenant_defs
defs = create_tenant_defs(Path(__file__).parent / "tenant.yaml")
```

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
