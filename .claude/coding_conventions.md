# 코딩 규칙

## 전체 가이드라인 및 스타일

- DOP(Dagster Open Platform) 코드베이스에서는 **함수형 프로그래밍 스타일**과 **불변 객체**를 선호합니다.
- 객체를 직접 수정하지 않고, 복사 후 특정 속성을 교체하는 방식을 사용합니다.
- 결과 객체는 `@record` 어노테이션 클래스를 사용합니다.
- `__init__.py` 파일에 임포트를 추가할 필요가 없습니다 (공개 API `@public` 어노테이션 제외). 내부 클래스와 함수는 절대 경로 임포트를 사용합니다.
- 프로덕션 코드에서 `print`를 사용하지 않습니다.

## Python 버전 요구사항

- **대상 Python 버전**: Python 3.9 이상
- **최신 Python 기능**: Python 3.9+ 기능을 적극 활용합니다.
- **타입 어노테이션**: 내장 제네릭 타입 사용 (예: `list[str]`, `dict[str, Any]`)

## 타입 어노테이션

- 모든 Python 코드에 타입 힌트 필수
- **반드시 내장 타입 사용**: `dict`, `list`, `set`, `tuple` (typing 모듈의 `Dict`, `List`, `Set`, `Tuple` 사용 금지)
- **절대 사용 금지**: `typing.Dict`, `typing.List`, `typing.Set`, `typing.Tuple`
- 모든 Python 코드는 `pyright` 타입 체크를 에러 없이 통과해야 합니다.

## 임포트 구성

- **항상 최상위(모듈 범위) 임포트 사용** - 함수 내부 임포트는 특정 경우에만 허용

### 함수 내부 임포트가 허용되는 경우

1. **TYPE_CHECKING 블록**: 타입 어노테이션에만 필요한 임포트
2. **순환 임포트 해결**: 순환 의존성을 발생시키는 임포트
3. **선택적 의존성**: 임포트 실패를 우아하게 처리해야 하는 경우
4. **고비용 지연 로딩**: 조건부로 사용되는 무거운 임포트

### 올바른 임포트 패턴 예시

```python
# 올바른 예: 최상위 임포트
import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_database_for_environment
from dagster_snowflake import SnowflakeResource

@dg.asset(group_name="analytics")
def my_asset(context: dg.AssetExecutionContext, snowflake: SnowflakeResource):
    database = get_database_for_environment()
    # 임포트를 직접 사용
```

```python
# 잘못된 예: 정당한 이유 없는 함수 내부 임포트
@dg.asset(group_name="analytics")
def my_asset(context: dg.AssetExecutionContext, snowflake: SnowflakeResource):
    from dagster_open_platform.utils.environment_helpers import get_database_for_environment
    database = get_database_for_environment()
```

```python
# 허용: TYPE_CHECKING 임포트
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagster import AssetExecutionContext
```

## 데이터 구조

- Dagster 코드베이스에서 데이터클래스, 경량 객체, 불변 객체에는 **항상 `@record` 사용**
  - **임포트**: `from dagster_shared.record import record`
  - `@record` 사용 대상: DTO, 결과 객체, 설정 객체, 에러/불일치 보고 객체, 분석 결과 등
  - `@dataclass` 사용 대상: 가변성이 필요한 경우, 외부 라이브러리 호환 필요 시

### @record 메서드 네이밍 규칙

- **`with_*` 네이밍**: 추가 데이터로 새 인스턴스를 반환하는 메서드
  - `result.with_error("message")` - 에러가 추가된 새 인스턴스 반환
  - `result.add_error("message")` - 사용 금지 (객체 변경처럼 보임)
- **이유**: `with_*` 패턴은 불변 패턴임을 명확하게 전달

## 서비스 아키텍처 패턴

### 싱글톤 패턴 금지

- 서비스나 상태 컴포넌트에 전역 싱글톤 객체 사용 금지
- 항상 서비스 인스턴스를 매개변수로 명시적 전달

```python
# 올바른 예: 인스턴스 기반 패턴
def create_service() -> MyService:
    return MyService()

def some_function(service: MyService):  # 명시적 의존성
    service.do_something()
```

### Literal 타입 사용

- 문자열 열거형에는 항상 `Literal` 타입 사용
- 재사용 가능한 리터럴 타입에는 타입 별칭 생성

```python
from typing import Literal

DiagnosticsLevel = Literal["off", "error", "info", "debug"]

def create_service(level: DiagnosticsLevel = "off") -> MyService:
    return MyService(level=level)
```

## 컨텍스트 매니저

**진입하지 않은 컨텍스트 매니저를 중간 변수에 할당하지 않기** - `with`에서 직접 사용:

```python
# 잘못된 예
run_context = build_context(sensor_name="test")
with run_context:
    pass

# 올바른 예
with build_context(sensor_name="test") as run_context:
    pass
```

**예외**: 종료 후 컨텍스트 매니저의 속성에 접근해야 하는 경우 허용

## 예외 처리 가이드라인

### 일반 원칙

- 기본적으로 예외를 제어 흐름으로 사용하지 않습니다.
- catch 블록에서 "대체" 동작을 구현하지 않습니다.
- 넓은 범위의 `Exception` 타입 캐치를 피합니다.

### 허용되는 예외 처리 용도

1. **에러 경계**: CLI 명령어, 에셋 실체화 작업 등
2. **API 호환성**: 예외를 제어 흐름에 사용하는 서드파티 API 보완
3. **예외 보강**: 재발생 전 컨텍스트 추가

### 사전 조건 확인 우선

```python
# 선호: 사전 조건 확인
if asset_key in asset_metadata:
    value = asset_metadata[asset_key]
else:
    handle_missing_asset()

# 지양: 예외를 통한 조건 발견
try:
    value = asset_metadata[asset_key]
except KeyError:
    handle_missing_asset()
```

### 딕셔너리/매핑 접근

- 항상 `in` 멤버십 테스트 사용 후 키 접근 (KeyError 캐치 대신)

### 예외 소멸 방지

- 예외를 조용히 삼키지 않습니다 - 항상 적절한 에러 경계까지 전파합니다.

```python
# 잘못된 예: 조용한 예외 소멸
try:
    process_data()
except (DataError, PartitionError):
    return  # 실제 문제를 숨김

# 올바른 예: 예외 전파
process_data()  # 예외가 적절한 경계까지 전파됨
```

## Dagster 에셋 패턴 (DOP 전용)

### 에셋 정의

```python
@dg.asset(
    tags={"dagster/kind/snowflake": ""},
    key=["purina", "oss_analytics", "table_name"],
    group_name="oss_analytics",
    automation_condition=dg.AutomationCondition.eager(),
    check_specs=[
        dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    ],
)
def my_asset(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """에셋의 비즈니스 목적을 설명하는 독스트링."""
    # 구현
    return dg.MaterializeResult(metadata={...})
```

### 메타데이터 반환

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

### 환경별 설정

```python
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)

database = get_database_for_environment()  # "PROD", "DEV" 등
schema = get_schema_for_environment("analytics")
```

## 문서화 스타일

- 모든 공개 함수와 클래스에 **Google 스타일 독스트링** 적용
- 해당하는 경우 Args, Returns, Raises 섹션 포함
- 복잡한 기능에는 독스트링에 예시 추가

## 패키지 구성

- 서브패키지 `__init__.py` 파일에서 `__all__` 사용 금지
- 최상위 패키지 `__init__.py`에서만 `__all__` 사용하여 공개 API 정의
- 심볼 재내보내기 시 명시적 임포트 패턴 사용: `import foo as foo`
