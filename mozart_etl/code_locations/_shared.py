"""Shared resources and config utilities for all code locations."""

import os
import re
import shutil
import sys
from pathlib import Path

import yaml

from mozart_etl.lib.storage.minio import S3Resource
from mozart_etl.lib.trino import TrinoResource


def get_shared_resources() -> dict:
    """Return the common resource definitions used across all code locations."""
    s3 = S3Resource(
        endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
        access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_REGION", "us-east-1"),
        bucket=os.getenv("S3_BUCKET_NAME", "warehouse"),
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
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    tenant = _resolve_config(raw["tenant"])
    tables = raw.get("tables", [])
    return tenant, tables


def find_dbt_executable() -> str:
    """Find the dbt executable, checking the venv Scripts dir on Windows."""
    found = shutil.which("dbt")
    if found:
        return found
    venv_scripts = Path(sys.executable).parent / "dbt.exe"
    if venv_scripts.exists():
        return str(venv_scripts)
    return "dbt"
