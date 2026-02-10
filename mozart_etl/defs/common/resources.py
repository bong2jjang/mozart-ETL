import os

import dagster as dg
from dagster.components import definitions

from mozart_etl.lib.storage.minio import MinIOResource


class TrinoResource(dg.ConfigurableResource):
    """Trino query engine resource for executing SQL queries."""

    host: str = os.getenv("TRINO_HOST", "localhost")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    schema_name: str = os.getenv("TRINO_SCHEMA", "default")
    user: str = os.getenv("TRINO_USER", "trino")

    def get_connection(self):
        from trino.dbapi import connect

        return connect(
            host=self.host,
            port=self.port,
            catalog=self.catalog,
            schema=self.schema_name,
            user=self.user,
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


minio = MinIOResource(
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    region=os.getenv("MINIO_REGION", "us-east-1"),
    bucket=os.getenv("MINIO_BUCKET", "mozart-data"),
)

trino = TrinoResource(
    host=os.getenv("TRINO_HOST", "localhost"),
    port=int(os.getenv("TRINO_PORT", "8080")),
    catalog=os.getenv("TRINO_CATALOG", "iceberg"),
    schema_name=os.getenv("TRINO_SCHEMA", "default"),
    user=os.getenv("TRINO_USER", "trino"),
)


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "minio": minio,
            "trino": trino,
        },
    )
