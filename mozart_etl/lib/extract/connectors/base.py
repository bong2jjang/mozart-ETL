from abc import ABC, abstractmethod

import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class BaseConnector(ABC):
    """Abstract base class for database connectors.

    Each connector implements connection and extraction logic for a specific
    database type using SQLAlchemy.
    """

    def __init__(self, config: dict):
        self.config = config
        self._engine: Engine | None = None

    @abstractmethod
    def get_connection_url(self) -> str:
        """Return the SQLAlchemy connection URL for this database type."""
        ...

    def get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.get_connection_url())
        return self._engine

    def extract_table(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        incremental_column: str | None = None,
        last_value: str | None = None,
        filters: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        """Extract a table from the source database into a PyArrow Table.

        Args:
            schema: Source schema name.
            table: Source table name.
            columns: Specific columns to select (None = all).
            incremental_column: Column to use for incremental loading.
            last_value: Last known value for incremental loading.
            filters: Key-value pairs for WHERE clause filtering (e.g. {"project_id": "..."}).
            limit: Optional row limit (for testing).

        Returns:
            PyArrow Table with the extracted data.
        """
        engine = self.get_engine()

        # Build SELECT clause
        qualified_table = f"{schema}.{table}" if schema else table
        select_cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {select_cols} FROM {qualified_table}"

        conditions = []
        params: dict = {}

        # Tenant-level filters (e.g. project_id)
        if filters:
            for i, (col, val) in enumerate(filters.items()):
                param_name = f"filter_{i}"
                conditions.append(f"{col} = :{param_name}")
                params[param_name] = val

        # Incremental filter
        if incremental_column and last_value:
            conditions.append(f"{incremental_column} > :last_value")
            params["last_value"] = last_value

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        if limit:
            query += f" LIMIT {limit}"

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            result_columns = list(result.keys())
            rows = result.fetchall()

        if not rows:
            # Return empty table with column names
            arrays = [pa.array([], type=pa.string()) for _ in result_columns]
            return pa.table(dict(zip(result_columns, arrays)))

        # Convert to PyArrow Table
        col_data = {col: [row[i] for row in rows] for i, col in enumerate(result_columns)}
        return pa.table(col_data)

    def test_connection(self) -> bool:
        """Test if the database connection is working."""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def close(self):
        """Close the database engine."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
