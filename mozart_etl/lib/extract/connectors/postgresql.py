from mozart_etl.lib.extract.connectors.base import BaseConnector


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector using psycopg2."""

    def get_connection_url(self) -> str:
        c = self.config
        host = c.get("host", "localhost")
        port = c.get("port", 5432)
        database = c.get("database", "postgres")
        username = c.get("username", "postgres")
        password = c.get("password", "")
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
