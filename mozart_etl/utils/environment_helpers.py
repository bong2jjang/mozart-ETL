import os


def get_environment() -> str:
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        return "BRANCH"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod":
        return "PROD"
    return "LOCAL"


def get_iceberg_schema_for_tenant(tenant_id: str) -> str:
    env = get_environment()
    if env == "LOCAL":
        return f"dev_{tenant_id}"
    return tenant_id


def get_dbt_target() -> str:
    env = get_environment()
    if env == "PROD":
        return "prod"
    return os.getenv("DBT_TARGET", "dev")
