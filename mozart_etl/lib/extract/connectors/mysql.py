from mozart_etl.lib.extract.connectors.base import BaseConnector


class MySQLConnector(BaseConnector):
    """MySQL database connector using mysql-connector-python."""

    def get_connection_url(self) -> str:
        c = self.config
        host = c.get("host", "localhost")
        port = c.get("port", 3306)
        database = c.get("database", "")
        username = c.get("username", "root")
        password = c.get("password", "")
        return f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
