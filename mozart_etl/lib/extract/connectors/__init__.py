from mozart_etl.lib.extract.connectors.base import BaseConnector
from mozart_etl.lib.extract.connectors.mysql import MySQLConnector
from mozart_etl.lib.extract.connectors.oracle import OracleConnector
from mozart_etl.lib.extract.connectors.postgresql import PostgreSQLConnector


def create_connector(source_config: dict) -> BaseConnector:
    """Factory function to create the appropriate connector based on source type."""
    connectors = {
        "postgresql": PostgreSQLConnector,
        "oracle": OracleConnector,
        "mysql": MySQLConnector,
    }

    source_type = source_config["type"]
    connector_cls = connectors.get(source_type)
    if connector_cls is None:
        raise ValueError(
            f"Unsupported source type: {source_type}. "
            f"Supported types: {list(connectors.keys())}"
        )

    return connector_cls(source_config)
