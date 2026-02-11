"""Code location for output layer (dbt mart models)."""

import shutil
import sys
from functools import cache
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslatorSettings, DbtCliResource, DbtProject, dbt_assets

from mozart_etl.code_locations._shared import get_shared_resources
from mozart_etl.lib.dbt.translator import CustomDagsterDbtTranslator
from mozart_etl.utils.environment_helpers import get_dbt_target


def _find_dbt_executable() -> str:
    """Find the dbt executable, checking the venv Scripts dir on Windows."""
    found = shutil.which("dbt")
    if found:
        return found
    # Fallback: look in the same venv as the running Python
    venv_scripts = Path(sys.executable).parent / "dbt.exe"
    if venv_scripts.exists():
        return str(venv_scripts)
    return "dbt"


@cache
def _dbt_project() -> DbtProject:
    project = DbtProject(
        project_dir=Path(__file__)
        .joinpath("..", "..", "..", "..", "mozart_etl_dbt")
        .resolve(),
        target=get_dbt_target(),
    )
    project.prepare_if_dev()
    return project


@cache
def _get_dbt_models():
    dbt_project = _dbt_project()

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


# Output pipeline job and schedule
output_job = dg.define_asset_job(
    name="output_pipeline",
    selection=dg.AssetSelection.groups("output"),
    tags={"pipeline": "output"},
)

output_schedule = dg.ScheduleDefinition(
    name="output_schedule",
    job=output_job,
    cron_schedule="0 3 * * *",
)

resources = get_shared_resources()
resources["dbt"] = DbtCliResource(
    project_dir=_dbt_project().project_dir,
    dbt_executable=_find_dbt_executable(),
)

defs = dg.Definitions(
    assets=[_get_dbt_models()],
    jobs=[output_job],
    schedules=[output_schedule],
    resources=resources,
)
