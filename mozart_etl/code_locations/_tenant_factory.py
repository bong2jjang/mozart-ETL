"""Factory to build per-tenant Dagster Definitions from tenant-specific YAML."""

import json
from functools import cache
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslatorSettings, DbtCliResource, DbtProject, dbt_assets

from mozart_etl.code_locations._shared import (
    find_dbt_executable,
    get_shared_resources,
    load_tenant_config,
)
from mozart_etl.lib.dbt.translator import TransformDagsterDbtTranslator
from mozart_etl.lib.extract.connectors import create_connector
from mozart_etl.lib.storage.minio import S3Resource
from mozart_etl.lib.trino import TrinoResource
from mozart_etl.utils.environment_helpers import get_dbt_target

TRANSFORM_DBT_DIR = Path(__file__).joinpath(
    "..", "..", "..", "mozart_etl_dbt_transform"
).resolve()


@cache
def _get_transform_dbt_project() -> DbtProject:
    """Shared DbtProject for all tenants.

    All tenants share the same manifest since the dbt models are identical.
    Per-tenant behavior comes from TransformDagsterDbtTranslator and --vars at runtime.
    """
    project = DbtProject(
        project_dir=TRANSFORM_DBT_DIR,
        target=get_dbt_target(),
    )
    project.prepare_if_dev()
    return project


def create_tenant_defs(tenant_config_path: Path) -> dg.Definitions:
    """Build a complete Definitions from a tenant-specific YAML file.

    Each tenant has its own tenant.yaml containing both connection info
    and table definitions. This ensures zero cross-tenant coupling.
    """
    tenant, tables = load_tenant_config(tenant_config_path)
    tenant_id = tenant["id"]

    assets: list = []
    for table in tables:
        assets.append(_create_extract_asset(tenant, table))

    dbt_transform = _create_dbt_transform_assets(tenant, tables)
    assets.append(dbt_transform)

    resources = get_shared_resources()
    resources["dbt"] = DbtCliResource(
        project_dir=TRANSFORM_DBT_DIR,
        dbt_executable=find_dbt_executable(),
    )

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
        resources=resources,
    )


def _create_extract_asset(tenant: dict, table: dict) -> dg.AssetsDefinition:
    """Create an asset that extracts data from source DB to S3 Parquet
    and loads it into a raw Iceberg table.

    Combines the old INPUT + RAW steps into a single asset.
    """
    tenant_id = tenant["id"]
    table_name = table["name"]
    source_config = tenant["source"]
    storage_config = tenant["storage"]
    iceberg_schema = tenant.get("iceberg", {}).get("schema", tenant_id)
    raw_schema = f"{iceberg_schema}_raw"

    @dg.asset(
        key=dg.AssetKey([tenant_id, "input", table_name]),
        group_name=tenant_id,
        tags={
            "dagster/kind/python": "",
            "dagster/kind/trino": "",
            "tenant": tenant_id,
            "pipeline": "input",
        },
        automation_condition=dg.AutomationCondition.on_cron(
            tenant.get("schedule", "0 */2 * * *")
        ),
    )
    def _extract(context: dg.AssetExecutionContext, s3: S3Resource, trino: TrinoResource):
        # 1) RDB → S3 Parquet
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
        finally:
            connector.close()

        # 2) S3 Parquet → Iceberg raw table
        bucket = storage_config["bucket"]
        prefix = storage_config["prefix"]
        s3a_path = f"s3a://{bucket}/{prefix}/{table_name}/"

        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS iceberg.{raw_schema}")
        full_table = f"iceberg.{raw_schema}.{table_name}"

        if table.get("mode") == "incremental":
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    dummy VARCHAR
                ) WITH (
                    external_location = '{s3a_path}',
                    format = 'PARQUET'
                )
            """)
        else:
            trino.execute_ddl(f"DROP TABLE IF EXISTS {full_table}")
            trino.execute_ddl(f"""
                CREATE TABLE {full_table}
                WITH (format = 'PARQUET')
                AS SELECT 1 as placeholder
            """)

        context.log.info(
            f"Extracted {arrow_table.num_rows} rows → {s3_path} → {full_table}"
        )
        return dg.MaterializeResult(
            metadata={
                "num_rows": dg.MetadataValue.int(arrow_table.num_rows),
                "s3_path": dg.MetadataValue.text(s3_path),
                "iceberg_table": dg.MetadataValue.text(full_table),
                "tenant": dg.MetadataValue.text(tenant_id),
            }
        )

    _extract.__name__ = f"input_{tenant_id}_{table_name}"
    _extract.__qualname__ = f"input_{tenant_id}_{table_name}"
    return _extract


def _create_dbt_transform_assets(
    tenant: dict, tables: list[dict]
) -> dg.AssetsDefinition:
    """Create dbt transform + output assets for a tenant.

    Uses the shared mozart_etl_dbt_transform project with --vars for
    per-tenant schema routing. Includes both transform and output models.
    """
    tenant_id = tenant["id"]
    table_names = [t["name"] for t in tables]
    dbt_project = _get_transform_dbt_project()

    translator = TransformDagsterDbtTranslator(
        tenant_id=tenant_id,
        settings=DagsterDbtTranslatorSettings(enable_code_references=False),
    )

    # Select transform models + corresponding mart output models
    select_parts = table_names + [f"mart_{n}" for n in table_names]
    select_str = " ".join(select_parts)

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=select_str,
        dagster_dbt_translator=translator,
        project=dbt_project,
    )
    def tenant_dbt_transform(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(
            ["build", "--vars", json.dumps({"tenant_id": tenant_id, **tenant.get("params", {})})],
            context=context,
        ).stream()

    tenant_dbt_transform.__name__ = f"dbt_transform_{tenant_id}"
    tenant_dbt_transform.__qualname__ = f"dbt_transform_{tenant_id}"
    return tenant_dbt_transform
