"""dbt assets for Mozart ETL output layer."""

from functools import cache

import dagster as dg
from dagster.components import definitions
from dagster_dbt import (
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from mozart_etl.defs.dbt.resources import mozart_etl_dbt_project
from mozart_etl.lib.dbt.translator import CustomDagsterDbtTranslator


@cache
def get_dbt_models():
    dbt_project = mozart_etl_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=CustomDagsterDbtTranslator(
            settings=DagsterDbtTranslatorSettings(enable_code_references=False)
        ),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_output_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return dbt_output_models


@definitions
def defs():
    return dg.Definitions(
        assets=[get_dbt_models()],
    )
