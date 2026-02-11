"""Factory to build per-tenant Dagster Definitions from tenant-specific YAML."""

from pathlib import Path

import dagster as dg

from mozart_etl.lib.extract.connectors import create_connector
from mozart_etl.lib.storage.minio import S3Resource
from mozart_etl.lib.trino import TrinoResource
from mozart_etl.code_locations._shared import get_shared_resources, load_tenant_config


def create_tenant_defs(tenant_config_path: Path) -> dg.Definitions:
    """Build a complete Definitions from a tenant-specific YAML file.

    Each tenant has its own tenant.yaml containing both connection info
    and table definitions. This ensures zero cross-tenant coupling.
    """
    tenant, tables = load_tenant_config(tenant_config_path)
    tenant_id = tenant["id"]

    assets = []
    for table in tables:
        assets.append(_create_extract_asset(tenant, table))
        assets.append(_create_transform_asset(tenant, table))

    job = dg.define_asset_job(
        name=f"{tenant_id}_pipeline",
        selection=dg.AssetSelection.groups(tenant_id),
        tags={"tenant": tenant_id, "pipeline": "tenant"},
    )

    schedule = dg.ScheduleDefinition(
        name=f"{tenant_id}_schedule",
        job=job,
        cron_schedule=tenant.get("schedule", "0 */2 * * *"),
    )

    return dg.Definitions(
        assets=assets,
        jobs=[job],
        schedules=[schedule],
        resources=get_shared_resources(),
    )


def _create_extract_asset(tenant: dict, table: dict) -> dg.AssetsDefinition:
    tenant_id = tenant["id"]
    table_name = table["name"]
    source_config = tenant["source"]
    storage_config = tenant["storage"]

    @dg.asset(
        key=dg.AssetKey([tenant_id, "input", table_name]),
        group_name=tenant_id,
        tags={
            "dagster/kind/python": "",
            "tenant": tenant_id,
            "pipeline": "input",
        },
        automation_condition=dg.AutomationCondition.on_cron(
            tenant.get("schedule", "0 */2 * * *")
        ),
    )
    def _extract(context: dg.AssetExecutionContext, s3: S3Resource):
        connector = create_connector(source_config)
        try:
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

            context.log.info(
                f"Extracted {arrow_table.num_rows} rows → {s3_path}"
            )
            return dg.MaterializeResult(
                metadata={
                    "num_rows": dg.MetadataValue.int(arrow_table.num_rows),
                    "s3_path": dg.MetadataValue.text(s3_path),
                    "tenant": dg.MetadataValue.text(tenant_id),
                }
            )
        finally:
            connector.close()

    _extract.__name__ = f"input_{tenant_id}_{table_name}"
    _extract.__qualname__ = f"input_{tenant_id}_{table_name}"
    return _extract


def _create_transform_asset(tenant: dict, table: dict) -> dg.AssetsDefinition:
    tenant_id = tenant["id"]
    table_name = table["name"]
    iceberg_schema = tenant.get("iceberg", {}).get("schema", tenant_id)
    storage_config = tenant["storage"]

    @dg.asset(
        key=dg.AssetKey([tenant_id, "transform", table_name]),
        deps=[dg.AssetKey([tenant_id, "input", table_name])],
        group_name=tenant_id,
        tags={
            "dagster/kind/trino": "",
            "dagster/kind/iceberg": "",
            "tenant": tenant_id,
            "pipeline": "transform",
        },
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _transform(context: dg.AssetExecutionContext, trino: TrinoResource, s3: S3Resource):
        bucket = storage_config["bucket"]
        prefix = storage_config["prefix"]
        s3_path = f"s3a://{bucket}/{prefix}/{table_name}/"

        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS iceberg.{iceberg_schema}")
        full_table = f"iceberg.{iceberg_schema}.{table_name}"

        if table.get("mode") == "incremental":
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    dummy VARCHAR
                ) WITH (
                    external_location = '{s3_path}',
                    format = 'PARQUET'
                )
            """)
            context.log.info(f"Iceberg table {full_table} synced from {s3_path}")
        else:
            trino.execute_ddl(f"DROP TABLE IF EXISTS {full_table}")
            trino.execute_ddl(f"""
                CREATE TABLE {full_table}
                WITH (format = 'PARQUET')
                AS SELECT 1 as placeholder
            """)

        return dg.MaterializeResult(
            metadata={
                "iceberg_table": dg.MetadataValue.text(full_table),
                "source_path": dg.MetadataValue.text(s3_path),
                "tenant": dg.MetadataValue.text(tenant_id),
            }
        )

    _transform.__name__ = f"transform_{tenant_id}_{table_name}"
    _transform.__qualname__ = f"transform_{tenant_id}_{table_name}"
    return _transform
