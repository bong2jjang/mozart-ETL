# 프로젝트 구조 가이드

## 개요

**Dagster Open Platform (DOP)**은 Dagster Labs의 내부 데이터 플랫폼으로, 분석 및 운영에 사용되는 실제 프로덕션 에셋을 포함합니다.

### 프로젝트의 두 가지 역할

1. Dagster Labs 내부 운영을 위한 **프로덕션 데이터 플랫폼**
2. Dagster로 데이터 플랫폼을 구축하는 **모범 사례 참고 자료**

### 핵심 기능

- 선언적 자동화(Declarative Automation) 실전 예제
- 신선도 검사(Freshness Checks) 및 에셋 상태 모니터링
- 메타데이터 및 데이터 품질 패턴
- 다중 소스 데이터 통합 (Fivetran, Segment, Snowflake, dbt, Sling 등)
- 재사용 가능한 컴포넌트 기반 아키텍처
- Git 통합 코드 참조

## 디렉토리 구조

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
│   │   └── ...                     # 기타 통합
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

DOP는 재사용 가능한 패턴을 위해 **Dagster 컴포넌트**를 광범위하게 활용합니다.

**컴포넌트 구조:**
```
defs/fivetran/
├── components/
│   └── component.yaml          # 컴포넌트 설정
└── py/
    ├── checks.py               # 에셋 체크
    └── definitions.py          # Python 기반 정의
```

**컴포넌트 YAML 예시:**
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

### 2. 에셋 정의 방식

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
    """에셋의 비즈니스 목적 설명."""
    return dg.MaterializeResult(metadata={...})
```

**B. 컴포넌트 기반 에셋:**
- `components/component.yaml`을 통한 YAML 설정으로 정의
- `lib/`의 컴포넌트 클래스가 인스턴스화 및 실행 담당
- 일반적인 패턴에 대해 선언적 설정 가능

### 3. 자동화 및 스케줄링

**선언적 자동화:**
```python
# 의존성 준비되면 즉시 실행
automation_condition=dg.AutomationCondition.eager()
# 매일 자정 실행
automation_condition=dg.AutomationCondition.on_cron("0 0 * * *")
```

**글로벌 신선도 정책:**
```python
# definitions.py 내
global_freshness_policy = InternalFreshnessPolicy.time_window(
    fail_window=timedelta(hours=36)
)
```

### 4. 에셋 체크 및 데이터 품질

```python
check_specs=[
    dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    dg.AssetCheckSpec("freshness_check", asset=asset_key),
]

@dg.asset_check(asset=asset_key, name="non_empty_check")
def check_non_empty(context, snowflake):
    result = snowflake.execute_query(f"SELECT COUNT(*) FROM {table}")
    passed = result[0][0] > 0
    return dg.AssetCheckResult(passed=passed, metadata={...})
```

### 5. 파티셔닝

```python
from dagster import DailyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(start_date="2024-01-01")

@dg.asset(partitions_def=daily_partition)
def incremental_asset(context: dg.AssetExecutionContext):
    window = context.asset_partitions_time_window_for_output()
    start_date = window.start
    end_date = window.end
```

### 6. 리소스

**주요 리소스:**
- `SnowflakeResource` - Snowflake 데이터베이스 접근
- `InsightsBigQueryResource` - Dagster Insights용 BigQuery
- `FivetranResource` - Fivetran API 통합
- `SegmentResource` - Segment API 접근
- 환경별 리소스 설정: `utils/environment_helpers.py` 참조

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
- 일반 쿼리용 컴포넌트 기반 패턴
- 연결 풀링 및 자격 증명 관리

### Sling
- 다양한 소스의 Sling 기반 데이터 수집
- 데이터 내보내기를 위한 이그레스(egress) 패턴
- 클라우드 제품 수집 유틸리티

### Segment
- Segment에서 이벤트 데이터 수집
- 실시간 및 배치 처리 패턴

## 주요 파일

- **`definitions.py`** - 루트 정의 진입점 (Dagster가 로드)
- **`lib/executable_component.py`** - 기본 컴포넌트 클래스
- **`utils/environment_helpers.py`** - 환경 설정
- **`utils/source_code.py`** - Git 코드 참조 통합
- **`pyproject.toml`** - 패키지 설정, 의존성, 도구 설정

## 새 에셋 생성 워크플로우

1. **위치 선택:** `defs/<통합명>/py/assets/` 또는 `defs/<통합명>/assets/`
2. **에셋 정의:** `@dg.asset` 데코레이터 사용
3. **체크 추가:** `check_specs`로 데이터 품질 검증 정의
4. **로컬 테스트:** `dagster dev` 또는 `just user-cloud` 사용
5. **포맷 및 확인:** `just ruff`와 `just pyright` 실행

## 새 컴포넌트 생성 워크플로우

1. **컴포넌트 클래스 정의**: `lib/<통합명>/component.py`에 작성
2. **pyproject.toml에 등록**: `[tool.dg.project]` > `registry_modules`에 추가
3. **YAML 설정 생성**: `defs/<통합명>/components/component.yaml`
4. **테스트 및 반복**

## 트러블슈팅

### 의존성 누락
- `just sync` 실행하여 워크스페이스 의존성 동기화
- `pyproject.toml`에서 필요한 패키지 확인

### 환경 변수 문제
- `component.yaml`에서 필요한 환경 변수 확인
- 셸이나 `.env` 파일에 변수가 설정되어 있는지 확인

### 타입 에러
- `just pyright` 실행하여 타입 문제 파악
- 리소스와 컨텍스트에 적절한 타입 어노테이션 확인

### 에셋 로딩 실패
- `definitions.py`가 에셋 모듈을 임포트하는지 확인
- 컴포넌트 YAML 구문 및 등록 확인
- `dagster dev` 로그에서 에러 확인
