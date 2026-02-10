import os
from pathlib import Path

import dagster as dg
import yaml
from jinja2 import Template

from mozart_etl.lib.extract.connectors import create_connector
from mozart_etl.lib.storage.minio import S3Resource


def _resolve_env_vars(value: str) -> str:
    """Resolve ${VAR_NAME:default} patterns in config values."""
    if not isinstance(value, str):
        return value

    import re

    pattern = r"\$\{(\w+)(?::([^}]*))?\}"

    def replacer(match):
        var_name = match.group(1)
        default = match.group(2) if match.group(2) is not None else ""
        return os.getenv(var_name, default)

    return re.sub(pattern, replacer, value)


def _resolve_config(config: dict) -> dict:
    """Recursively resolve environment variables in a config dict."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = _resolve_config(value)
        elif isinstance(value, str):
            resolved[key] = _resolve_env_vars(value)
        else:
            resolved[key] = value
    return resolved


def load_tenants_config(config_path: Path | None = None) -> list[dict]:
    """Load and resolve tenant configurations from YAML."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "tenants.yaml"

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    tenants = []
    for tenant in raw.get("tenants", []):
        tenants.append(_resolve_config(tenant))
    return tenants


def load_tables_config(config_path: Path | None = None) -> list[dict]:
    """Load table extraction configurations from YAML."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "tables.yaml"

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return raw.get("tables", [])


class TenantPipelineFactory:
    """Factory that generates Dagster assets for each tenant x table combination.

    Reads tenant and table configurations from YAML files and dynamically
    creates extraction assets that pull data from source databases into
    MinIO as Parquet files.
    """

    def __init__(
        self,
        tenants_config: Path | None = None,
        tables_config: Path | None = None,
    ):
        self.tenants = load_tenants_config(tenants_config)
        self.tables = load_tables_config(tables_config)

    def build_extraction_assets(self) -> list[dg.AssetsDefinition]:
        """Create extraction assets for each tenant x table combination."""
        assets = []
        for tenant in self.tenants:
            for table in self.tables:
                asset = self._create_extract_asset(tenant, table)
                assets.append(asset)
        return assets

    def _create_extract_asset(self, tenant: dict, table: dict) -> dg.AssetsDefinition:
        """Create a single extraction asset: Source DB → MinIO/Parquet."""
        tenant_id = tenant["id"]
        table_name = table["name"]
        source_config = tenant["source"]
        storage_config = tenant["storage"]

        asset_key = dg.AssetKey(["raw", tenant_id, table_name])

        @dg.asset(
            key=asset_key,
            group_name=f"extract_{tenant_id}",
            tags={
                "dagster/kind/python": "",
                "tenant": tenant_id,
                "pipeline": "extract",
            },
            automation_condition=dg.AutomationCondition.on_cron(
                tenant.get("schedule", "0 */2 * * *")
            ),
        )
        def _extract(context: dg.AssetExecutionContext, s3: S3Resource):
            connector = create_connector(source_config)
            try:
                # Build tenant-level filters from params + table's tenant_filter
                filters = None
                tenant_filter_col = table.get("tenant_filter")
                tenant_params = tenant.get("params", {})
                if tenant_filter_col and tenant_filter_col in tenant_params:
                    filters = {tenant_filter_col: tenant_params[tenant_filter_col]}

                arrow_table = connector.extract_table(
                    schema=table["source_schema"],
                    table=table["source_table"],
                    columns=table.get("columns"),
                    incremental_column=table.get("incremental_column"),
                    filters=filters,
                )

                s3_path = s3.write_parquet(
                    table=arrow_table,
                    prefix=storage_config["prefix"],
                    table_name=table_name,
                )

                num_rows = arrow_table.num_rows
                context.log.info(
                    f"Extracted {num_rows} rows from {tenant_id}.{table_name} → {s3_path}"
                )

                return dg.MaterializeResult(
                    metadata={
                        "num_rows": dg.MetadataValue.int(num_rows),
                        "s3_path": dg.MetadataValue.text(s3_path),
                        "tenant": dg.MetadataValue.text(tenant_id),
                        "source_type": dg.MetadataValue.text(source_config["type"]),
                    }
                )
            finally:
                connector.close()

        # Rename the function for better Dagster UI display
        _extract.__name__ = f"extract_{tenant_id}_{table_name}"
        _extract.__qualname__ = f"extract_{tenant_id}_{table_name}"

        return _extract

    def build_schedules(self) -> list[dg.ScheduleDefinition]:
        """Create per-tenant extraction schedules."""
        schedules = []
        for tenant in self.tenants:
            tenant_id = tenant["id"]
            cron = tenant.get("schedule", "0 */2 * * *")

            # Select all assets for this tenant
            job = dg.define_asset_job(
                name=f"extract_{tenant_id}_job",
                selection=dg.AssetSelection.groups(f"extract_{tenant_id}"),
                tags={"tenant": tenant_id, "pipeline": "extract"},
            )

            schedule = dg.ScheduleDefinition(
                name=f"extract_{tenant_id}_schedule",
                job=job,
                cron_schedule=cron,
            )
            schedules.append(schedule)

        return schedules
