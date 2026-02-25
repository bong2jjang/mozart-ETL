"""Shared resources and config utilities for all code locations."""

import logging
import os
import re
import shutil
import sys
from pathlib import Path

import yaml

from mozart_etl.lib.storage.minio import S3Resource
from mozart_etl.lib.trino import TrinoResource

logger = logging.getLogger(__name__)


def get_shared_resources() -> dict:
    """Return the common resource definitions used across all code locations."""
    logger.info(
        "[shared] Initializing resources (Trino=%s:%s, S3=%s)",
        os.getenv("TRINO_HOST", "localhost"),
        os.getenv("TRINO_PORT", "8080"),
        os.getenv("S3_ENDPOINT_URL", ""),
    )
    s3 = S3Resource(
        endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
        access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_REGION", "us-east-1"),
        bucket=os.getenv("S3_BUCKET_NAME", "warehouse"),
        verify_ssl=os.getenv("S3_VERIFY_SSL", "true").lower() == "true",
    )

    trino = TrinoResource(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema_name=os.getenv("TRINO_SCHEMA", "default"),
        user=os.getenv("TRINO_USER", "trino"),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )

    return {
        "s3": s3,
        "minio": s3,
        "trino": trino,
    }


def _resolve_env_vars(value: str) -> str:
    """Resolve ${VAR_NAME:default} patterns in config values."""
    if not isinstance(value, str):
        return value

    pattern = r"\$\{(\w+)(?::([^}]*))?\}"

    def replacer(match):
        var_name = match.group(1)
        default = match.group(2) if match.group(2) is not None else ""
        return os.getenv(var_name, default)

    return re.sub(pattern, replacer, value)


def _resolve_config(config: dict) -> dict:
    """Recursively resolve environment variables in a config dict."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = _resolve_config(value)
        elif isinstance(value, str):
            resolved[key] = _resolve_env_vars(value)
        else:
            resolved[key] = value
    return resolved


def load_tenant_config(config_path: Path) -> tuple[dict, list[dict]]:
    """Load a single tenant's config from its own YAML file.

    Returns:
        (tenant_dict, tables_list) - resolved with env vars
    """
    logger.info("[shared] Loading tenant config: %s", config_path)
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    tenant = _resolve_config(raw["tenant"])
    tables = raw.get("tables", [])
    table_names = [t["name"] for t in tables]
    logger.info(
        "[shared] Tenant '%s' loaded: %d tables (%s)",
        tenant["id"], len(tables), ", ".join(table_names),
    )
    return tenant, tables


def find_dbt_executable() -> str:
    """Find the dbt executable, checking the venv Scripts dir on Windows.

    On Windows with Git Bash, uses a wrapper script (.venv/Scripts/dbt.bat)
    that calls a bash script which executes dbt in Docker to avoid Windows
    compatibility issues.
    """
    # Check venv Scripts directory
    # On Windows, prefer .bat wrapper which is recognized as executable
    scripts_dir = Path(sys.executable).parent
    if sys.platform == "win32":
        venv_dbt = scripts_dir / "dbt.bat"
        if venv_dbt.exists():
            absolute_path = str(venv_dbt.resolve())
            logger.info("[shared] dbt found in venv: %s", absolute_path)
            return absolute_path

    # Unix or fallback
    venv_dbt = scripts_dir / "dbt"
    if venv_dbt.exists():
        absolute_path = str(venv_dbt.resolve())
        logger.info("[shared] dbt found in venv: %s", absolute_path)
        return absolute_path

    # Check PATH
    found = shutil.which("dbt")
    if found:
        absolute_path = str(Path(found).resolve())
        logger.info("[shared] dbt found via PATH: %s", absolute_path)
        return absolute_path

    logger.warning("[shared] dbt not found, falling back to 'dbt'")
    return "dbt"
