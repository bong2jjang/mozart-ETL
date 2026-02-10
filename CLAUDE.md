# Dagster Open Platform (DOP)

이 문서는 Dagster Open Platform 코드베이스 작업에 대한 종합적인 가이드를 제공합니다.

## 개요

**Dagster Open Platform**은 Dagster Labs의 내부 데이터 플랫폼으로, 분석 및 운영에 사용되는 실제 프로덕션 에셋을 포함합니다. 이 프로젝트는 다음 두 가지 역할을 수행합니다:
1. Dagster Labs 내부 운영을 위한 프로덕션 데이터 플랫폼
2. Dagster를 활용한 데이터 플랫폼 구축의 모범 사례 참고 자료

**주요 기능:**
- 선언적 자동화(Declarative Automation) 실전 예제
- 신선도 검사(Freshness Checks) 및 에셋 상태 모니터링
- 메타데이터 및 데이터 품질 패턴
- 다중 소스 데이터 통합 (Fivetran, Segment, Snowflake, dbt, Sling 등)
- 재사용 가능한 컴포넌트 기반 아키텍처
- Git 통합 코드 참조

## 저장소 구조

```
dagster-open-platform/
├── dagster_open_platform/          # 메인 패키지
│   ├── definitions.py              # 루트 정의 파일 (진입점)
│   ├── defs/                       # 개별 파이프라인 정의
│   │   ├── aws/                    # AWS 관련 파이프라인
│   │   ├── claude/                 # Claude AI 관련 파이프라인
│   │   ├── dbt/                    # dbt 변환 파이프라인
│   │   ├── fivetran/               # Fivetran 동기화 정의
│   │   ├── pypi/                   # PyPI 다운로드 분석
│   │   ├── segment/                # Segment 이벤트 데이터
│   │   ├── snowflake/              # Snowflake 네이티브 에셋
│   │   ├── sling/                  # Sling 데이터 수집
│   │   ├── stripe/                 # Stripe 결제 데이터
│   │   └── ...                     # 기타 다수 통합
│   ├── lib/                        # 공유 라이브러리 코드
│   │   ├── executable_component.py # 기본 컴포넌트 클래스
│   │   ├── schedule.py             # 스케줄 유틸리티
│   │   ├── dbt/                    # dbt 통합 헬퍼
│   │   ├── fivetran/               # Fivetran 컴포넌트 구현
│   │   ├── snowflake/              # Snowflake 컴포넌트 헬퍼
│   │   ├── sling/                  # Sling 유틸리티
│   │   └── ...                     # 기타 통합 라이브러리
│   └── utils/                      # 유틸리티 함수
│       ├── environment_helpers.py  # 환경별 설정
│       ├── source_code.py          # Git 코드 참조 연결
│       └── github_gql_queries.py   # GitHub GraphQL 쿼리
├── dagster_open_platform_dbt/      # dbt 프로젝트 디렉토리
├── dagster_open_platform_tests/    # 테스트 스위트
├── dagster_open_platform_eu/       # EU 전용 배포 설정
├── pyproject.toml                  # 패키지 설정
└── README.md                       # 공개 문서
```

## 핵심 개념

### 1. 컴포넌트 기반 아키텍처

DOP는 재사용 가능한 패턴을 위해 **Dagster 컴포넌트**를 광범위하게 활용합니다. 컴포넌트는 `lib/`에서 정의되고 `defs/`의 YAML 설정을 통해 인스턴스화됩니다.

**컴포넌트 구조:**
```
defs/fivetran/
├── components/
│   └── component.yaml          # 컴포넌트 설정
└── py/
    ├── checks.py               # 에셋 체크
    └── definitions.py          # Python 기반 정의
```

**컴포넌트 YAML 예시 (component.yaml):**
```yaml
type: dagster_open_platform.lib.FivetranComponent

attributes:
  workspace:
    account_id: "{{ env('FIVETRAN_ACCOUNT_ID') }}"
    api_key: "{{ env('FIVETRAN_API_KEY') }}"
  translation:
    key: "fivetran/{{ props.table }}"
    automation_condition: "{{ hourly_if_not_in_progress }}"
```

**컴포넌트 구현 (`lib/`):**
- `lib/executable_component.py`의 기본 클래스를 확장하는 커스텀 컴포넌트
- 통합별 로직 처리 (Fivetran, Segment, Snowflake 등)
- `pyproject.toml`의 `[tool.dg.project]` > `registry_modules`에 등록

### 2. 에셋 정의

에셋은 두 가지 방식으로 정의할 수 있습니다:

**A. Python 기반 에셋 (`py/` 디렉토리):**
```python
@dg.asset(
    tags={"dagster/kind/snowflake": ""},
    key=["purina", "oss_analytics", "dagster_pypi_downloads"],
    group_name="oss_analytics",
    partitions_def=oss_analytics_daily_partition,
    automation_condition=dg.AutomationCondition.eager(),
    check_specs=[
        dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    ],
)
def my_asset(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """비즈니스 맥락을 포함한 에셋 설명."""
    # 구현
    return dg.MaterializeResult(metadata={...})
```

**B. 컴포넌트 기반 에셋:**
- `components/component.yaml`의 YAML 설정으로 정의
- `lib/`의 컴포넌트 클래스가 인스턴스화 및 실행 처리
- 일반적인 패턴에 대한 선언적 설정 가능

### 3. 자동화 및 스케줄링

**선언적 자동화:**
```python
automation_condition=dg.AutomationCondition.eager()  # 의존성 준비되면 즉시 실행
automation_condition=dg.AutomationCondition.on_cron("0 0 * * *")  # 매일 자정 실행
```

**글로벌 신선도 정책:**
```python
# definitions.py 내
global_freshness_policy = InternalFreshnessPolicy.time_window(fail_window=timedelta(hours=36))
```

**스케줄 패턴:**
- `defs/schedules/` 또는 통합별 디렉토리에 정의
- 일반 패턴: cron 기반, 온디맨드, 신선도 기반

### 4. 에셋 체크 및 데이터 품질

**에셋 체크 패턴:**
```python
check_specs=[
    dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    dg.AssetCheckSpec("freshness_check", asset=asset_key),
    dg.AssetCheckSpec("schema_validation", asset=asset_key),
]

# 체크 구현
@dg.asset_check(asset=asset_key, name="non_empty_check")
def check_non_empty(context, snowflake):
    result = snowflake.execute_query(f"SELECT COUNT(*) FROM {table}")
    passed = result[0][0] > 0
    return dg.AssetCheckResult(passed=passed, metadata={...})
```

### 5. 메타데이터 및 관측성

**메타데이터 추가:**
```python
return dg.MaterializeResult(
    metadata={
        "num_rows": dg.MetadataValue.int(row_count),
        "last_updated": dg.MetadataValue.timestamp(current_time),
        "query": dg.MetadataValue.md(query_text),
        "preview": dg.MetadataValue.table(df_preview),
    }
)
```

### 6. 파티셔닝

**일반적인 파티션 정의:**
```python
# 일별 파티션
from dagster import DailyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(start_date="2024-01-01")

# 에셋 내에서 접근
def my_asset(context: dg.AssetExecutionContext):
    window = context.asset_partitions_time_window_for_output()
    start_date = window.start
    end_date = window.end
```

### 7. 리소스

**주요 리소스:**
- `SnowflakeResource` - Snowflake 데이터베이스 접근
- `InsightsBigQueryResource` - Dagster Insights용 BigQuery
- `FivetranResource` - Fivetran API 통합
- `SegmentResource` - Segment API 접근
- `utils/environment_helpers.py`를 통한 환경별 리소스 설정

## 통합 패턴

### Fivetran
- `lib/fivetran/component.py`의 컴포넌트 기반 통합
- 외부 데이터 소스를 Snowflake로 동기화
- 자동 신선도 검사 및 연결 테스트

### dbt
- `dagster_open_platform_dbt/`의 전체 dbt 프로젝트 통합
- `lib/dbt/translator.py`의 커스텀 변환기
- dbt DAG에서 에셋 의존성 자동 매핑

### Snowflake
- 데이터 변환용 네이티브 Snowflake 에셋
- 일반 쿼리를 위한 컴포넌트 기반 패턴
- 연결 풀링 및 자격 증명 관리

### Sling
- 다양한 소스에서 Sling 기반 데이터 수집
- 데이터 내보내기를 위한 이그레스(egress) 패턴
- 클라우드 제품 수집 유틸리티

### Segment
- Segment에서 이벤트 데이터 수집
- 실시간 및 배치 처리 패턴

## 개발 워크플로우

### 새 에셋 생성

1. **위치 선택:** `defs/<통합명>/py/assets/` 또는 `defs/<통합명>/assets/`
2. **에셋 정의:**
   ```python
   @dg.asset(
       key=["database", "schema", "table_name"],
       group_name="my_group",
       automation_condition=dg.AutomationCondition.eager(),
   )
   def my_new_asset(context, snowflake):
       """이 에셋이 무엇을 하는지 명확한 설명."""
       # 구현
       return dg.MaterializeResult(metadata={...})
   ```
3. **체크 추가:** 데이터 품질을 위한 `check_specs` 정의
4. **로컬 테스트:** `dagster dev` 또는 `just user-cloud` 사용
5. **포맷 및 확인:** `just ruff`와 `just pyright` 실행

### 새 컴포넌트 생성

1. **컴포넌트 클래스 정의**: `lib/<통합명>/component.py`에 작성:
   ```python
   from dagster.components import Component, component

   @component(name="my_component")
   class MyComponent(Component):
       def build_defs(self, context):
           # Definitions 객체 반환
           return dg.Definitions(assets=[...])
   ```
2. **pyproject.toml에 등록:**
   ```toml
   [tool.dg.project]
   registry_modules = [
       "dagster_open_platform.lib.*",
   ]
   ```
3. **YAML 설정 생성**: `defs/<통합명>/components/component.yaml`
4. **테스트 및 반복**

### 환경 설정

**환경 헬퍼:**
```python
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)

database = get_database_for_environment()  # "PROD", "DEV" 등
schema = get_schema_for_environment("analytics")
```

**환경 변수:**
- 필요한 환경 변수는 `component.yaml`의 `requirements.env`에 문서화
- 컴포넌트 YAML에서 `{{ env('VAR_NAME') }}`으로 템플릿 사용

### 테스트

**테스트 구조:**
```
dagster_open_platform_tests/
├── unit/           # 유틸리티 및 컴포넌트 단위 테스트
├── integration/    # 외부 시스템 통합 테스트
└── fixtures/       # 공유 테스트 픽스처
```

**테스트 실행:**
```bash
# dagster-open-platform 디렉토리에서
pytest dagster_open_platform_tests/

# 특정 테스트 실행
pytest dagster_open_platform_tests/unit/test_my_module.py

# 커버리지 포함
pytest --cov=dagster_open_platform dagster_open_platform_tests/
```

**테스트 모범 사례:**
- `responses` 또는 `mock`을 사용하여 외부 API 호출 모킹
- 복잡한 출력에 대해 `syrupy`로 스냅샷 테스트
- 컴포넌트 인스턴스화 및 정의 생성 테스트
- 모의(mock) 리소스로 에셋 실행 테스트

### 코드 참조

프로젝트는 `definitions.py`의 `link_defs_code_references_to_git()`을 사용하여 코드를 자동으로 Git에 연결합니다. 이를 통해:
- Dagster UI에서 소스 코드로 클릭 이동
- 배포된 에셋의 버전 추적
- 향상된 디버깅 및 협업

## 공통 패턴

### 패턴: 다중 환경 배포

에셋이 헬퍼를 사용하여 환경(prod, dev, staging)에 적응:
```python
database = get_database_for_environment()
schema = get_schema_for_environment("my_schema")
```

### 패턴: 증분 로딩

대용량 데이터셋에 파티션과 증분 로직 사용:
```python
@dg.asset(partitions_def=daily_partition)
def incremental_asset(context):
    window = context.asset_partitions_time_window_for_output()
    # 파티션 시간 윈도우에 해당하는 데이터만 쿼리
```

### 패턴: 에셋 의존성

의존성을 명시적으로 선언:
```python
@dg.asset(deps=[upstream_asset_key])
def downstream_asset(context):
    # 이 에셋은 upstream_asset_key에 의존
```

또는 키 참조를 통해:
```python
upstream_data = context.resources.snowflake.fetch_table(upstream_key)
```

### 패턴: 에셋 그룹

관련 에셋을 구성:
```python
@dg.asset(group_name="oss_analytics")
def pypi_downloads(): ...

@dg.asset(group_name="oss_analytics")
def github_metrics(): ...
```

## 주요 파일

- **`definitions.py`** - Dagster가 로드하는 루트 정의 진입점
- **`lib/executable_component.py`** - 기본 컴포넌트 클래스
- **`utils/environment_helpers.py`** - 환경 설정
- **`utils/source_code.py`** - Git 코드 참조 통합
- **`pyproject.toml`** - 패키지 설정, 의존성, 도구 설정

## 개발 명령어

저장소 루트에서 실행:

```bash
# 환경 동기화
just sync

# 코드 포맷팅
just ruff

# 타입 체크
just pyright

# 전체 검증 실행
just check

# 로컬 Dagster UI 실행
just user-cloud  # 사용자 대면 클라우드
just host-cloud  # 호스트 대면 클라우드

# 테스트 실행
pytest dagster_open_platform_tests/
```

## 트러블슈팅

### 의존성 누락
- `just sync`를 실행하여 모든 워크스페이스 의존성 동기화
- `pyproject.toml`에서 필요한 패키지 확인

### 환경 변수
- `component.yaml`에서 필요한 환경 변수 확인
- 셸이나 `.env` 파일에 변수가 설정되어 있는지 확인

### 타입 에러
- `just pyright`를 실행하여 타입 문제 파악
- 리소스와 컨텍스트에 적절한 타입 어노테이션 확인

### 에셋 로딩 실패
- `definitions.py`가 에셋 모듈을 임포트하는지 확인
- 컴포넌트 YAML 구문 및 등록 확인
- `dagster dev` 로그에서 에러 확인

## 추가 자료

- **메인 문서:** 이 디렉토리의 `README.md` 참조
- **Dagster 공식 문서:** https://docs.dagster.io
- **OSS 저장소:** `../dagster` (형제 의존성)
- **내부 도구:** `just dagster-labs` (관리 유틸리티)

## Claude Code 참고 사항

이 코드베이스로 작업할 때:
1. **기존 패턴 따르기** - 유사한 에셋/컴포넌트를 참고
2. **타입 힌트 사용** - 모든 코드는 pyright를 위해 적절한 타입 지정
3. **메타데이터 추가** - 관측성을 위해 MaterializeResult에 풍부한 메타데이터 포함
4. **에셋 문서화** - 비즈니스 목적을 설명하는 명확한 독스트링 작성
5. **철저한 테스트** - 새 컴포넌트 및 복잡한 에셋에 테스트 추가
6. **환경 확인** - 다중 환경 지원을 위해 환경 헬퍼 사용
7. **커밋 전 포맷팅** - 커밋 전에 항상 `just ruff` 실행
