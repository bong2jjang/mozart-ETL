"""Shared resources and utilities for all code locations."""

import os

import dagster as dg

from mozart_etl.defs.common.resources import TrinoResource
from mozart_etl.lib.storage.minio import S3Resource


def get_shared_resources() -> dict:
    """Return the common resource definitions used across all code locations."""
    s3 = S3Resource(
        endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
        access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_REGION", "us-east-1"),
        bucket=os.getenv("S3_BUCKET_NAME", "data-lake"),
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
