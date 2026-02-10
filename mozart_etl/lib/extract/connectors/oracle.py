from mozart_etl.lib.extract.connectors.base import BaseConnector


class OracleConnector(BaseConnector):
    """Oracle database connector using oracledb."""

    def get_connection_url(self) -> str:
        c = self.config
        host = c.get("host", "localhost")
        port = c.get("port", 1521)
        username = c.get("username", "")
        password = c.get("password", "")

        # Oracle can use service_name or SID
        service_name = c.get("service_name")
        sid = c.get("sid")

        if service_name:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service_name})))"
        elif sid:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SID={sid})))"
        else:
            dsn = f"{host}:{port}"

        return f"oracle+oracledb://{username}:{password}@{dsn}"
