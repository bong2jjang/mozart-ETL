from functools import cache
from pathlib import Path

from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtCliResource, DbtProject
from mozart_etl.utils.environment_helpers import get_dbt_target


@cache
def mozart_etl_dbt_project() -> DbtProject:
    project = DbtProject(
        project_dir=Path(__file__)
        .joinpath("..", "..", "..", "..", "mozart_etl_dbt")
        .resolve(),
        target=get_dbt_target(),
    )
    project.prepare_if_dev()
    return project


@definitions
def defs():
    dbt_project = mozart_etl_dbt_project()
    assert dbt_project
    return Definitions(resources={"dbt": DbtCliResource(project_dir=dbt_project.project_dir)})
