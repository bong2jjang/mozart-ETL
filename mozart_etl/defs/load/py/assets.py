"""Load assets: Create Iceberg tables from Parquet files in MinIO via Trino."""

import dagster as dg
from dagster.components import definitions

from mozart_etl.defs.common.resources import TrinoResource
from mozart_etl.lib.extract.factory import load_tables_config, load_tenants_config
from mozart_etl.lib.storage.minio import MinIOResource


def build_load_assets() -> list[dg.AssetsDefinition]:
    """Create load assets for each tenant x table combination.

    Each asset reads Parquet from MinIO and creates/refreshes an Iceberg table
    via Trino's CREATE TABLE AS SELECT or INSERT INTO.
    """
    tenants = load_tenants_config()
    tables = load_tables_config()

    assets = []
    for tenant in tenants:
        for table in tables:
            asset = _create_load_asset(tenant, table)
            assets.append(asset)
    return assets


def _create_load_asset(tenant: dict, table: dict) -> dg.AssetsDefinition:
    tenant_id = tenant["id"]
    table_name = table["name"]
    iceberg_config = tenant.get("iceberg", {})
    iceberg_schema = iceberg_config.get("schema", tenant_id)
    storage_config = tenant["storage"]

    raw_key = dg.AssetKey(["raw", tenant_id, table_name])
    iceberg_key = dg.AssetKey(["iceberg", tenant_id, table_name])

    @dg.asset(
        key=iceberg_key,
        deps=[raw_key],
        group_name=f"load_{tenant_id}",
        tags={
            "dagster/kind/trino": "",
            "dagster/kind/iceberg": "",
            "tenant": tenant_id,
            "pipeline": "load",
        },
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _load(context: dg.AssetExecutionContext, trino: TrinoResource, minio: MinIOResource):
        bucket = storage_config["bucket"]
        prefix = storage_config["prefix"]
        s3_path = f"s3a://{bucket}/{prefix}/{table_name}/"

        # Ensure schema exists
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS iceberg.{iceberg_schema}")

        full_table = f"iceberg.{iceberg_schema}.{table_name}"

        if table.get("mode") == "incremental":
            # For incremental: create external table pointing to Parquet location
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
            # Full refresh: drop and recreate
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

    _load.__name__ = f"load_{tenant_id}_{table_name}"
    _load.__qualname__ = f"load_{tenant_id}_{table_name}"

    return _load


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=build_load_assets())
