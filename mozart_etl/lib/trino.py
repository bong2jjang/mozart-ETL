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

    def query_preview(self, table_name: str, limit: int = 5) -> tuple[list[str], list[tuple]]:
        """Query sample rows with column names for UI preview.

        Returns:
            (column_names, rows) tuple.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return columns, rows
        finally:
            conn.close()
