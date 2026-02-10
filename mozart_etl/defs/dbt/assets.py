import json
from datetime import timedelta
from functools import cache
from typing import Optional

import dagster as dg
from dagster.components import definitions
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)
from dagster_dbt.asset_utils import DBT_DEFAULT_SELECT

from mozart_etl.defs.dbt.partitions import insights_partition
from mozart_etl.defs.dbt.resources import mozart_etl_dbt_project
from mozart_etl.lib.dbt.translator import CustomDagsterDbtTranslator

INCREMENTAL_SELECTOR = "config.materialized:incremental"

logger = dg.get_dagster_logger()


@cache
def get_dbt_non_partitioned_models(
    custom_translator: Optional[DagsterDbtTranslator] = None,
):
    dbt_project = mozart_etl_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=False)
            )
        ),
        select=DBT_DEFAULT_SELECT,
        exclude=INCREMENTAL_SELECTOR,
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_non_partitioned_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return dbt_non_partitioned_models


class DbtConfig(dg.Config):
    full_refresh: bool = False


@cache
def get_dbt_partitioned_models(
    custom_translator: Optional[DagsterDbtTranslator] = None,
):
    dbt_project = mozart_etl_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=INCREMENTAL_SELECTOR,
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=False)
            )
        ),
        partitions_def=insights_partition,
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_partitioned_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        dbt_vars = {
            "min_date": (context.partition_time_window.start - timedelta(hours=3)).isoformat(),
            "max_date": context.partition_time_window.end.isoformat(),
        }

        args = (
            ["build", "--full-refresh"]
            if config.full_refresh
            else ["build", "--vars", json.dumps(dbt_vars)]
        )

        yield from dbt.cli(args, context=context).stream()

    return dbt_partitioned_models


@definitions
def defs():
    dbt_partitioned_models = get_dbt_partitioned_models()
    dbt_non_partitioned_models = get_dbt_non_partitioned_models()

    return dg.Definitions(
        assets=[
            dbt_partitioned_models,
            dbt_non_partitioned_models,
        ],
    )
