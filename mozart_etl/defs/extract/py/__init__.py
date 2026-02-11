import dagster as dg
from dagster.components import definitions

from mozart_etl.lib.extract.factory import TenantPipelineFactory


@definitions
def defs() -> dg.Definitions:
    factory = TenantPipelineFactory()
    extraction_assets = factory.build_extraction_assets()

    return dg.Definitions(
        assets=extraction_assets,
    )
