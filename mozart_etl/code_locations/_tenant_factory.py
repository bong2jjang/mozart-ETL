"""Factory to build per-tenant Dagster Definitions from tenant-specific YAML."""

import json
import logging
from functools import cache
from pathlib import Path

import dagster as dg
import pyarrow as pa
from dagster import Output
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

logger = logging.getLogger(__name__)

TRANSFORM_DBT_DIR = Path(__file__).joinpath(
    "..", "..", "..", "mozart_etl_dbt_transform"
).resolve()

CODE_LOCATIONS_DIR = Path(__file__).parent

PREVIEW_MAX_ROWS = 5


def _pyarrow_to_trino_type(pa_type) -> str:
    """Map a PyArrow type to a Trino SQL type string."""
    if pa.types.is_boolean(pa_type):
        return "BOOLEAN"
    if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type):
        return "SMALLINT"
    if pa.types.is_int32(pa_type):
        return "INTEGER"
    if pa.types.is_int64(pa_type):
        return "BIGINT"
    if pa.types.is_float32(pa_type):
        return "REAL"
    if pa.types.is_float64(pa_type):
        return "DOUBLE"
    if pa.types.is_decimal(pa_type):
        return f"DECIMAL({pa_type.precision}, {pa_type.scale})"
    if pa.types.is_date(pa_type):
        return "DATE"
    if pa.types.is_timestamp(pa_type):
        return "TIMESTAMP"
    if pa.types.is_time(pa_type):
        return "TIME"
    return "VARCHAR"


def _build_column_defs(arrow_schema) -> str:
    """Build SQL column definitions from a PyArrow schema."""
    cols = []
    for field in arrow_schema:
        trino_type = _pyarrow_to_trino_type(field.type)
        cols.append(f'    "{field.name}" {trino_type}')
    return ",\n".join(cols)


def _build_arrow_preview(arrow_table: pa.Table) -> dict:
    """Build column schema + sample rows metadata from a PyArrow Table."""
    columns = [
        dg.TableColumn(name=field.name, type=str(field.type))
        for field in arrow_table.schema
    ]
    sample = arrow_table.slice(0, min(PREVIEW_MAX_ROWS, arrow_table.num_rows))
    df = sample.to_pandas()
    return {
        "dagster/column_schema": dg.TableSchema(columns=columns),
        "preview": dg.MetadataValue.md(df.to_markdown(index=False)),
    }


def _build_trino_preview(trino: TrinoResource, relation_name: str) -> dict:
    """Query Trino for sample rows and return markdown preview metadata."""
    try:
        columns, rows = trino.query_preview(relation_name, limit=PREVIEW_MAX_ROWS)
        if not rows:
            return {}
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        body = "\n".join(
            "| " + " | ".join(str(v) for v in row) + " |"
            for row in rows
        )
        return {"preview": dg.MetadataValue.md(f"{header}\n{separator}\n{body}")}
    except Exception as e:
        logger.warning("Failed to build Trino preview for %s: %s", relation_name, e)
        return {}


@cache
def _get_transform_dbt_project() -> DbtProject:
    """Shared DbtProject for all tenants.

    All tenants share the same manifest since model-paths includes all tenant dirs.
    Per-tenant behavior comes from TransformDagsterDbtTranslator and --vars at runtime.
    """
    project = DbtProject(
        project_dir=TRANSFORM_DBT_DIR,
        target=get_dbt_target(),
    )
    project.prepare_if_dev()
    return project


def _get_tenant_dbt_select(tenant_id: str) -> str:
    """Build dbt select string by scanning tenant's models directory.

    Returns space-separated model names for @dbt_assets(select=...).
    """
    models_dir = CODE_LOCATIONS_DIR / tenant_id / "models"
    if not models_dir.exists():
        return ""
    model_names = []
    for sql_file in models_dir.rglob("*.sql"):
        if not sql_file.stem.startswith("_"):
            model_names.append(sql_file.stem)
    return " ".join(sorted(model_names))


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
    if dbt_transform is not None:
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

        # 2) S3 Parquet → Iceberg raw table (via Hive bridge)
        #    Iceberg doesn't support external_location, so we:
        #    a) Create a temporary Hive external table pointing to S3 Parquet
        #    b) CTAS or INSERT INTO Iceberg from the Hive table
        #    c) Drop the temporary Hive table
        s3_dir = s3_path.replace("s3://", "s3a://").rsplit("/", 1)[0] + "/"
        col_defs = _build_column_defs(arrow_table.schema)

        hive_schema = f"hive.{raw_schema}"
        hive_bridge = f"hive.{raw_schema}.__bridge_{table_name}"
        full_table = f"iceberg.{raw_schema}.{table_name}"

        # 2a) Hive external table → read S3 Parquet
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {hive_schema}")
        trino.execute_ddl(f"DROP TABLE IF EXISTS {hive_bridge}")
        trino.execute_ddl(f"""
            CREATE TABLE {hive_bridge} (
{col_defs}
            ) WITH (
                external_location = '{s3_dir}',
                format = 'PARQUET'
            )
        """)

        # 2b) Iceberg table from Hive bridge
        trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS iceberg.{raw_schema}")

        if table.get("mode") == "incremental":
            # First run: create table; subsequent: append
            trino.execute_ddl(f"""
                CREATE TABLE IF NOT EXISTS {full_table}
                WITH (format = 'PARQUET')
                AS SELECT * FROM {hive_bridge} WHERE 1=0
            """)
            trino.execute_ddl(f"DELETE FROM {full_table}")
            trino.execute_ddl(f"INSERT INTO {full_table} SELECT * FROM {hive_bridge}")
        else:
            trino.execute_ddl(f"DROP TABLE IF EXISTS {full_table}")
            trino.execute_ddl(f"""
                CREATE TABLE {full_table}
                WITH (format = 'PARQUET')
                AS SELECT * FROM {hive_bridge}
            """)

        # 2c) Cleanup temporary Hive bridge table
        trino.execute_ddl(f"DROP TABLE IF EXISTS {hive_bridge}")

        context.log.info(
            f"Extracted {arrow_table.num_rows} rows → {s3_path} → {full_table}"
        )
        preview_meta = _build_arrow_preview(arrow_table)
        return dg.MaterializeResult(
            metadata={
                "num_rows": dg.MetadataValue.int(arrow_table.num_rows),
                "s3_path": dg.MetadataValue.text(s3_path),
                "iceberg_table": dg.MetadataValue.text(full_table),
                "tenant": dg.MetadataValue.text(tenant_id),
                **preview_meta,
            }
        )

    _extract.__name__ = f"input_{tenant_id}_{table_name}"
    _extract.__qualname__ = f"input_{tenant_id}_{table_name}"
    return _extract


def _create_dbt_transform_assets(
    tenant: dict, tables: list[dict]
) -> dg.AssetsDefinition | None:
    """Create dbt staging + mart assets for a tenant.

    Scans the tenant's models/ directory to discover dbt model files,
    then uses @dbt_assets to register them with Dagster.
    """
    tenant_id = tenant["id"]
    dbt_project = _get_transform_dbt_project()

    select_str = _get_tenant_dbt_select(tenant_id)
    if not select_str:
        return None

    translator = TransformDagsterDbtTranslator(
        tenant_id=tenant_id,
        settings=DagsterDbtTranslatorSettings(enable_code_references=False),
    )

    manifest_data = json.loads(dbt_project.manifest_path.read_text())

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=select_str,
        dagster_dbt_translator=translator,
        project=dbt_project,
    )
    def tenant_dbt_transform(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, trino: TrinoResource
    ):
        invocation = dbt.cli(
            ["build", "--vars", json.dumps({"tenant_id": tenant_id, **tenant.get("params", {})})],
            context=context,
        )
        for event in invocation.stream().fetch_row_counts().fetch_column_metadata():
            if isinstance(event, Output) and "unique_id" in event.metadata:
                unique_id = event.metadata["unique_id"].text
                node = manifest_data["nodes"].get(unique_id)
                if node and node["config"]["materialized"] != "view":
                    relation_name = node.get("relation_name")
                    if relation_name:
                        sample_meta = _build_trino_preview(trino, relation_name)
                        if sample_meta:
                            event = event.with_metadata(
                                {**event.metadata, **sample_meta}
                            )
            yield event

    tenant_dbt_transform.__name__ = f"dbt_transform_{tenant_id}"
    tenant_dbt_transform.__qualname__ = f"dbt_transform_{tenant_id}"
    return tenant_dbt_transform
