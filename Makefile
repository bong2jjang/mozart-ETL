.PHONY: sync dev validate dbt-parse check-sync

# 테넌트 동기화 (workspace.yaml + __init__.py + dbt_project.yml 자동 생성)
sync:
	python scripts/sync_tenants.py

# dbt 파싱 (동기화 후 실행)
dbt-parse: sync
	cd mozart_etl_dbt_transform && dbt parse && cd ..

# 개발 서버 시작 (동기화 + 파싱 후 실행)
dev: dbt-parse
	dagster dev -w workspace.yaml

# 정의 검증 (동기화 + 파싱 후 실행)
validate: dbt-parse
	dagster definitions validate -w workspace.yaml

# CI: 동기화 상태 확인
check-sync:
	python scripts/sync_tenants.py --check
