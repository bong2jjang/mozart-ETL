---
description: 변경된 파일의 독스트링을 검증합니다
---

# /validate_changed_docstrings

## 목표

변경된 Python 파일의 독스트링이 Google 스타일을 따르는지 검증하고, 누락된 독스트링을 식별합니다.

## 실행 단계

### 1단계: 변경 파일 식별

```bash
git diff --name-only --diff-filter=ACMR HEAD -- '*.py'
```

### 2단계: 각 파일 분석

변경된 각 Python 파일에서:

1. **에셋 함수**: `@dg.asset` 데코레이터가 있는 함수에 독스트링이 있는지 확인
2. **공개 함수/클래스**: 공개 API에 독스트링이 있는지 확인
3. **형식 검증**: Google 스타일 독스트링 준수 여부 확인
   - Args 섹션: 모든 매개변수 문서화
   - Returns 섹션: 반환값 문서화
   - Raises 섹션: 발생 가능한 예외 문서화

### 3단계: 보고서 생성

```md
## 독스트링 검증 결과

### 누락된 독스트링
- `파일경로:줄번호` - `함수명`: 독스트링 없음

### 형식 위반
- `파일경로:줄번호` - `함수명`: [위반 내용]

### 권장 독스트링
[누락된 독스트링에 대한 제안]
```

## 독스트링 예시 (DOP 에셋)

```python
@dg.asset(group_name="oss_analytics")
def dagster_pypi_downloads(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """PyPI에서 Dagster 패키지 다운로드 통계를 수집합니다.

    매일 PyPI 다운로드 데이터를 Snowflake에 적재하여
    OSS 사용 현황 분석에 활용합니다.

    Args:
        context: Dagster 에셋 실행 컨텍스트.
        snowflake: Snowflake 데이터베이스 리소스.

    Returns:
        행 수와 마지막 업데이트 시간이 포함된 MaterializeResult.
    """
```

## 주의사항

- 내부(private) 함수(`_`로 시작)의 독스트링은 선택사항입니다.
- 에셋 함수의 독스트링은 비즈니스 목적을 설명해야 합니다.
- 변경하지 않은 코드의 독스트링은 건드리지 않습니다.
