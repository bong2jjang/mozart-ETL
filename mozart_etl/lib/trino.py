"""Trino query engine resource."""

import logging

import dagster as dg

logger = logging.getLogger(__name__)


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
        sql_preview = sql.strip().replace("\n", " ")[:120]
        logger.info("[Trino] execute: %s", sql_preview)
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            logger.info("[Trino] execute: returned %d rows", len(rows))
            return rows
        finally:
            conn.close()

    def execute_ddl(self, sql: str):
        sql_preview = sql.strip().replace("\n", " ")[:120]
        logger.info("[Trino] DDL: %s", sql_preview)
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
        logger.info("[Trino] preview: SELECT * FROM %s LIMIT %d", table_name, limit)
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            logger.info("[Trino] preview: %d columns, %d rows", len(columns), len(rows))
            return columns, rows
        finally:
            conn.close()
