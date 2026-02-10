# 개발 워크플로우

## Ruff 사용 (필수)

**중요**: Python 파일을 편집(Write, Edit)한 **매번** 반드시 즉시 다음을 수행합니다:

1. **ruff 포맷팅/린팅 실행** (git 저장소 루트 디렉토리에서):

   ```bash
   just ruff
   ```

   Python 파일을 편집할 **때마다** 반드시 실행합니다.

2. **수동 수정 필요 시**: ruff가 자동으로 수정하지 못한 에러가 있으면 수동으로 수정합니다.

3. **성공 확인**: `just ruff`가 깨끗하게 통과하는지 확인한 후 작업 완료로 간주합니다.

### 스타일 가이드라인

- ruff 포매터로 코드 포맷팅 (black 대체)
- 임포트 정렬: 임포트 결합, 절대 경로 임포트만 사용
- 100자 줄 길이 제한 및 기타 스타일 규칙 적용

### 수동 수정이 필요한 일반적인 경우

ruff가 자동 수정하지 못하는 문제:

- **줄 길이 위반**: 긴 줄을 적절히 분리
- **임포트 구성**: 복잡한 임포트 정렬 문제
- **타입 힌트**: 누락된 타입 어노테이션 추가
- **독스트링 포맷**: Google 스타일 독스트링 확인
- **미사용 임포트/변수**: 제거 또는 수정

### 워크플로우 예시

```bash
# Python 파일 편집 후 저장소 루트에서:
just ruff

# 에러가 남아있으면:
# 1. 에러 메시지 읽기
# 2. 필요한 수동 수정 적용
# 3. just ruff 다시 실행
# 4. 깨끗할 때까지 반복
```

## Pyright 타입 체크

Python 파일 편집 후 타입 에러를 확인합니다:

```bash
just pyright
```

- 모든 코드가 pyright 타입 체크를 에러 없이 통과해야 합니다.
- 리소스와 컨텍스트에 적절한 타입 어노테이션을 확인합니다.

## Pytest 사용

- 항상 저장소 루트에서 pytest를 실행하고, 테스트 파일의 전체 경로를 지정합니다.

```bash
# 전체 테스트 실행
pytest dagster_open_platform_tests/

# 특정 테스트 파일 실행
pytest dagster_open_platform_tests/unit/test_my_module.py

# 커버리지 포함
pytest --cov=dagster_open_platform dagster_open_platform_tests/
```

## 전체 검증

모든 변경사항이 완료되면 전체 검증을 실행합니다:

```bash
just check
```

이 명령은 ruff + pyright + 기타 검증을 한 번에 실행합니다.

## 로컬 Dagster UI 실행

```bash
# 사용자 대면 클라우드
just user-cloud

# 호스트 대면 클라우드
just host-cloud
```

## 환경 동기화

의존성을 동기화해야 할 때:

```bash
just sync
```

## 일반적인 개발 순서

1. 코드 작성/수정
2. `just ruff` 실행 (포맷팅 + 린팅)
3. `just pyright` 실행 (타입 체크)
4. `pytest` 실행 (관련 테스트)
5. `just check` 실행 (전체 검증)
6. 필요 시 수동 수정 후 2~5 반복

## 커밋 전 체크리스트

- [ ] `just ruff` 통과
- [ ] `just pyright` 통과
- [ ] 관련 테스트 통과
- [ ] 새 에셋/컴포넌트에 독스트링 작성
- [ ] 메타데이터가 `MaterializeResult`에 포함
- [ ] 환경 헬퍼 사용 (하드코딩된 환경값 없음)
