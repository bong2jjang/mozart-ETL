import dagster as dg
from dagster.components import definitions
from dagster_dbt import DbtManifestAssetSelection

from mozart_etl.defs.dbt.assets import CustomDagsterDbtTranslator, get_dbt_non_partitioned_models
from mozart_etl.defs.dbt.partitions import insights_partition
from mozart_etl.defs.dbt.resources import mozart_etl_dbt_project


@definitions
def defs():
    dbt_analytics_core_job = dg.define_asset_job(
        name="dbt_analytics_core_job",
        selection=DbtManifestAssetSelection.build(
            manifest=mozart_etl_dbt_project().manifest_path,
            dagster_dbt_translator=CustomDagsterDbtTranslator(),
        ).required_multi_asset_neighbors(),
        tags={
            "team": "data",
            "dbt_pipeline": "analytics_core",
            "dagster/max_retries": 1,
        },
    )

    @dg.schedule(cron_schedule="0 3 * * *", job=dbt_analytics_core_job)
    def dbt_analytics_core_schedule():
        most_recent_partition = insights_partition.get_last_partition_key()
        yield dg.RunRequest(
            partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
        )

    return dg.Definitions(
        schedules=[dbt_analytics_core_schedule],
    )
