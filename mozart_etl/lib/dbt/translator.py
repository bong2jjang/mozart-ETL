from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator


class TransformDagsterDbtTranslator(DagsterDbtTranslator):
    """Per-tenant dbt translator for TRANSFORM + OUTPUT layers.

    Asset key mapping:
    - Sources (raw): [tenant_id, "input", table_name]
    - Transform models: [tenant_id, "transform", model_name]
    - Output models: [tenant_id, "output", model_name]
    """

    def __init__(self, tenant_id: str, settings=None):
        self._tenant_id = tenant_id
        super().__init__(settings=settings)

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == "source":
            return dg.AssetKey([self._tenant_id, "input", name])

        fqn = dbt_resource_props.get("fqn", [])
        if len(fqn) >= 2 and fqn[1] == "output":
            return dg.AssetKey([self._tenant_id, "output", name])

        return dg.AssetKey([self._tenant_id, "transform", name])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return self._tenant_id

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[dg.AutomationCondition]:
        return dg.AutomationCondition.eager()
