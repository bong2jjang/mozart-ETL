"""Unified pipeline jobs and schedules for tenant and output pipelines."""

import dagster as dg
from dagster.components import definitions

from mozart_etl.lib.extract.factory import load_tenants_config


def build_jobs_and_schedules() -> tuple[list, list]:
    """Create per-tenant jobs (input + transform) and an output pipeline."""
    tenants = load_tenants_config()
    jobs = []
    schedules = []

    for tenant in tenants:
        tenant_id = tenant["id"]
        cron = tenant.get("schedule", "0 */2 * * *")

        # Per-tenant job: covers both input and transform assets (same group)
        job = dg.define_asset_job(
            name=f"{tenant_id}_pipeline",
            selection=dg.AssetSelection.groups(tenant_id),
            tags={"tenant": tenant_id, "pipeline": "tenant"},
        )
        jobs.append(job)

        schedule = dg.ScheduleDefinition(
            name=f"{tenant_id}_schedule",
            job=job,
            cron_schedule=cron,
        )
        schedules.append(schedule)

    # Output pipeline: all dbt mart assets
    output_job = dg.define_asset_job(
        name="output_pipeline",
        selection=dg.AssetSelection.groups("output"),
        tags={"pipeline": "output"},
    )
    jobs.append(output_job)

    output_schedule = dg.ScheduleDefinition(
        name="output_schedule",
        job=output_job,
        cron_schedule="0 3 * * *",
    )
    schedules.append(output_schedule)

    return jobs, schedules


@definitions
def defs() -> dg.Definitions:
    jobs, schedules = build_jobs_and_schedules()
    return dg.Definitions(jobs=jobs, schedules=schedules)
