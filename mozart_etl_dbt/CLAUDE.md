# Mozart ETL dbt 프로젝트

## 개요

이 dbt 프로젝트는 Mozart ETL의 **OUTPUT 레이어**를 담당합니다.
테넌트별로 격리된 Iceberg 테이블을 UNION ALL하여 통합 mart 테이블을 생성합니다.

**엔진**: dbt-trino (Trino SQL 쿼리 엔진)

> 현재 등록된 테넌트 목록: `dbt_project.yml`의 `vars.tenants` 참조

## 데이터 흐름

```
{tenant_A}.{table} (Iceberg) ──┐
{tenant_B}.{table} (Iceberg) ──┼─→ mart_{table}_all (UNION ALL + tenant_id 컬럼)
{tenant_C}.{table} (Iceberg) ──┘
        ...
```

upstream(Iceberg 테이블)은 Dagster의 transform 에셋이 생성합니다.
이 dbt 프로젝트는 이들을 집계하는 mart 모델만 포함합니다.

## 프로젝트 구조

```
mozart_etl_dbt/
├── dbt_project.yml           # 프로젝트 설정, vars.tenants 정의
├── profiles.yml              # Trino 연결 (dev: 인증없음, prod: LDAP)
├── dependencies.yml          # 외부 패키지 (dbt_utils 등)
├── models/
│   ├── staging/
│   │   └── sources.yml       # 테넌트별 Iceberg source 선언
│   └── mart/
│       ├── mart_*.sql        # 통합 mart 모델
│       └── schema.yml        # 모델 문서 + 테스트
└── macros/
    ├── union_tenants.sql     # ★ 핵심: 테넌트 UNION ALL 매크로
    ├── get_custom_schema.sql # 스키마 이름 결정 로직
    ├── get_custom_database.sql
    ├── hash_string.sql       # SHA-512 해시 유틸
    ├── limit_dates_for_dev.sql     # dev에서 날짜 범위 제한
    ├── limit_dates_for_insights.sql
    └── query_to_list.sql     # SQL 결과 → Jinja 리스트
```

## 핵심 매크로: union_tenants

`dbt_project.yml`의 `vars.tenants` 목록을 순회하며 모든 테넌트의 동일 테이블을 UNION ALL합니다.
`tenant_id` 컬럼이 자동으로 추가됩니다.

```sql
-- 사용법: {{ union_tenants('{table_name}') }}
-- 결과: SELECT '{tenant_A}' AS tenant_id, t.* FROM source(...) UNION ALL SELECT '{tenant_B}' ...
```

테넌트가 추가/제거되면 `dbt_project.yml`의 `vars.tenants`만 수정하면 이 매크로를 사용하는 모든 mart에 자동 반영됩니다.

## 모델 레이어

### sources (staging/sources.yml)

테넌트별 Iceberg 테이블을 dbt source로 선언합니다.
`meta.dagster.asset_key`로 Dagster의 transform 에셋과 DAG를 연결합니다.

```yaml
# 패턴: 각 테넌트마다 source 블록 1개
sources:
  - name: {tenant_id}
    database: iceberg
    schema: {tenant_id}
    tables:
      - name: {table_name}
        meta:
          dagster:
            asset_key: ["{tenant_id}", "transform", "{table_name}"]
```

### mart (models/mart/)

테넌트 데이터를 통합한 최종 테이블입니다. `materialized: table`로 설정됩니다.

```sql
-- mart_{table_name}_all.sql (패턴)
{{ config(materialized='table') }}
{{ union_tenants('{table_name}') }}
```

## Dagster 연동

- `CustomDagsterDbtTranslator`가 dbt 리소스를 Dagster 에셋으로 매핑
- mart 모델 → Asset Key: `["output", model_name]`, Group: `"output"`
- source → `meta.dagster.asset_key` 값 사용 (upstream transform 에셋 연결)
- 자동화: mart 모델은 deps 업데이트 시 자동 실행

## 프로필 (profiles.yml)

| 프로필 | 인증 | 스레드 | 용도 |
|--------|------|--------|------|
| dev | none (인증 없음) | 4 | 로컬 개발 |
| prod | LDAP | 8 | 운영 환경 |

환경변수: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`, `DBT_SCHEMA`, `DBT_TARGET`

## 개발 명령어

```bash
dbt deps          # 패키지 설치
dbt parse         # manifest 생성
dbt build         # 모델 빌드 + 테스트
dbt run           # 모델만 실행
dbt test          # 테스트만 실행
```

## 확장 규칙

### 신규 테넌트 추가 시

1. `models/staging/sources.yml`에 테넌트 source 블록 추가
2. `dbt_project.yml`의 `vars.tenants` 목록에 추가

→ 기존 `union_tenants` 사용 mart 모델에 새 테넌트가 자동 포함됩니다.

### 신규 mart 모델 추가 시

1. `models/mart/mart_{name}.sql` 생성 — `{{ union_tenants('{table}') }}` 사용
2. `models/mart/schema.yml`에 모델 문서 + 테스트 추가
3. (필요시) `sources.yml`에 새 테이블 source 추가 (모든 테넌트에)

## 규칙

- **mart 모델만 작성**: staging/intermediate 모델 불필요 (upstream은 Dagster가 처리)
- **`union_tenants` 매크로 활용**: 테넌트 통합이 필요한 모든 mart에서 사용
- **schema.yml 동기화**: mart 모델 변경 시 schema.yml의 컬럼 정의/테스트도 업데이트
- **`meta.dagster.asset_key` 필수**: 모든 source table에 Dagster 에셋 키 메타 명시
