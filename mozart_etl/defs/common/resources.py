import os

import dagster as dg
from dagster.components import definitions

from mozart_etl.lib.storage.minio import S3Resource


class TrinoResource(dg.ConfigurableResource):
    """Trino query engine resource for executing SQL queries."""

    host: str = os.getenv("TRINO_HOST", "localhost")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    schema_name: str = os.getenv("TRINO_SCHEMA", "default")
    user: str = os.getenv("TRINO_USER", "trino")
    http_scheme: str = os.getenv("TRINO_HTTP_SCHEME", "http")

    def get_connection(self):
        from trino.dbapi import connect

        return connect(
            host=self.host,
            port=self.port,
            catalog=self.catalog,
            schema=self.schema_name,
            user=self.user,
            http_scheme=self.http_scheme,
        )

    def execute(self, sql: str, params=None) -> list:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            conn.close()

    def execute_ddl(self, sql: str):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
        finally:
            conn.close()


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


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "s3": s3,
            "minio": s3,  # backward compatibility alias
            "trino": trino,
        },
    )
