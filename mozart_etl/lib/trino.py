"""Trino query engine resource."""

import dagster as dg


class TrinoResource(dg.ConfigurableResource):
    """Trino query engine resource for executing SQL queries."""

    host: str = "localhost"
    port: int = 8080
    catalog: str = "iceberg"
    schema_name: str = "default"
    user: str = "trino"
    http_scheme: str = "http"

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
