# Mozart ETL - 멀티테넌트 ETL 파이프라인

## 개요

**Mozart ETL**은 Dagster 기반의 멀티테넌트 ETL 파이프라인 프로젝트입니다.
RDB(PostgreSQL, Oracle, MySQL)에서 데이터를 추출하여 MinIO(Parquet)에 저장하고,
dbt-trino로 변환하며, Trino/Iceberg에 적재합니다.

## 아키텍처

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Tenant A (PG)  │────▶│                  │────▶│                  │
├─────────────────┤     │  MinIO (Parquet)  │     │  Trino/Iceberg   │
│  Tenant B (ORA) │────▶│  s3://raw/{tid}/  │────▶│  iceberg.{tid}.  │
├─────────────────┤     │  {table}/         │     │  {table}         │
│  Tenant C (MY)  │────▶│  YYYY-MM-DD/      │     │                  │
└─────────────────┘     └──────────────────┘     └──────────────────┘
      Extract              Land (Parquet)         Transform + Serve
   (Python/SQLAlchemy)                            (dbt-trino + Python)
```

## 저장소 구조

```
mozart_etl/
├── definitions.py                 # 루트 Dagster 정의 (진입점)
├── config/
│   ├── tenants.yaml               # 테넌트 설정 (DB 연결 정보)
│   └── tables.yaml                # 추출 대상 테이블 정의
├── defs/
│   ├── common/resources.py        # 공통 리소스 (MinIO, Trino)
│   ├── extract/py/                # 추출 에셋 (RDB → MinIO/Parquet)
│   ├── load/py/                   # 적재 에셋 (Parquet → Iceberg)
│   ├── transform/python/          # Python 변환 에셋
│   └── dbt/                       # dbt-trino 에셋/스케줄
├── lib/
│   ├── executable_component.py    # 베이스 컴포넌트
│   ├── schedule.py                # 스케줄 컴포넌트
│   ├── dbt/translator.py          # Trino용 dbt 번역기
│   ├── extract/
│   │   ├── factory.py             # 멀티테넌트 팩토리
│   │   └── connectors/            # DB 커넥터 (PG, Oracle, MySQL)
│   └── storage/minio.py           # MinIO 리소스
└── utils/
    └── environment_helpers.py     # 환경 헬퍼

mozart_etl_dbt/
├── dbt_project.yml                # dbt-trino 프로젝트 설정
├── profiles.yml                   # Trino 연결 프로필
├── models/
│   ├── staging/                   # 스테이징 모델 (view)
│   ├── intermediate/              # 중간 모델 (view)
│   └── mart/                      # 마트 모델 (table)
└── macros/                        # Trino 호환 매크로

docker/
├── trino/catalog/iceberg.properties  # Trino Iceberg 카탈로그
└── postgres/init.sql                 # 샘플 DB 초기화

docker-compose.yml                 # MinIO + Trino + Iceberg REST + PostgreSQL
```

## 로컬 개발 시작

```bash
# 1. Python 환경 설정
uv venv --python 3.12
uv sync

# 2. Docker 인프라 시작
docker compose up -d

# 3. dbt 의존성 설치 및 파싱
cd mozart_etl_dbt && dbt deps && dbt parse && cd ..

# 4. Dagster 시작
dagster dev
```

## 핵심 패턴

### 멀티테넌트 팩토리

`config/tenants.yaml`에 테넌트를 정의하면 `TenantPipelineFactory`가
각 테넌트 × 테이블 조합에 대해 추출/적재 에셋을 자동 생성합니다.

### 리소스

- **MinIOResource**: S3 호환 MinIO 스토리지 (Parquet 읽기/쓰기)
- **TrinoResource**: Trino 쿼리 엔진 (DDL/DML 실행)
- **DbtCliResource**: dbt-trino 실행

### 환경 설정

`.env` 파일에서 환경변수를 로드합니다. `config/tenants.yaml`에서
`${VAR_NAME:default}` 패턴으로 환경변수를 참조합니다.
