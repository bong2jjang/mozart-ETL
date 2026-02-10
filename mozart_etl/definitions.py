import warnings
from datetime import timedelta

from dagster import AssetSelection, FreshnessPolicy, apply_freshness_policy
from dagster._utils.warnings import BetaWarning, PreviewWarning

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg

global_freshness_policy = FreshnessPolicy.time_window(fail_window=timedelta(hours=36))


@dg.components.definitions
def defs() -> dg.Definitions:
    import mozart_etl.defs

    defs = dg.components.load_defs(mozart_etl.defs)

    defs = defs.permissive_map_resolved_asset_specs(
        func=lambda spec: apply_freshness_policy(spec, global_freshness_policy),
        selection=AssetSelection.all().materializable(),
    )

    return defs
