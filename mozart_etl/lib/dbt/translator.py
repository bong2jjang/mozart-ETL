from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator


class TransformDagsterDbtTranslator(DagsterDbtTranslator):
    """Per-tenant dbt translator for staging + mart layers.

    Strips tenant prefix from model names and maps to Dagster asset keys:
    - Sources:            [tenant_id, "input", table_name]
    - Staging models:     [tenant_id, "staging", stg_model_name]
    - Mart models:        [tenant_id, "output", mart_model_name]

    Naming convention:
    - dbt model name: {tenant_id}__{layer}_{entity}  (e.g. project_01__stg_cfg_item_master)
    - asset key name: {layer}_{entity}                (e.g. stg_cfg_item_master)
    """

    def __init__(self, tenant_id: str, settings=None):
        self._tenant_id = tenant_id
        self._prefix = f"{tenant_id}__"
        super().__init__(settings=settings)

    def _strip_prefix(self, name: str) -> str:
        """Remove tenant prefix: 'project_01__mart_item_master' → 'mart_item_master'."""
        if name.startswith(self._prefix):
            return name[len(self._prefix):]
        return name

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        # Sources → [tenant_id, "input", table_name]
        if resource_type == "source":
            return dg.AssetKey([self._tenant_id, "input", name])

        clean_name = self._strip_prefix(name)

        # Mart models → [tenant_id, "output", mart_X]
        if clean_name.startswith("mart_"):
            return dg.AssetKey([self._tenant_id, "output", clean_name])

        # Staging models → [tenant_id, "staging", stg_X]
        return dg.AssetKey([self._tenant_id, "staging", clean_name])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return self._tenant_id

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[dg.AutomationCondition]:
        return dg.AutomationCondition.eager()
