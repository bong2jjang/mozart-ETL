from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtProject

asset_is_new_or_updated = ~dg.AutomationCondition.in_progress() & (
    dg.AutomationCondition.code_version_changed() | dg.AutomationCondition.missing()
)

asset_is_new_or_updated_or_deps_updated = ~dg.AutomationCondition.in_progress() & (
    dg.AutomationCondition.code_version_changed()
    | dg.AutomationCondition.missing()
    | dg.AutomationCondition.any_deps_updated()
)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """dbt-trino translator for Mozart ETL.

    Translates dbt resource properties into Dagster asset specs with
    Trino/Iceberg-specific conventions.
    """

    def _get_group_name_for_resource(self, dbt_props: Mapping[str, Any]) -> str:
        resource_type = dbt_props["resource_type"]
        if resource_type == "snapshot":
            return "snapshots"

        fqn = dbt_props.get("fqn", [])
        asset_path = fqn[2:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def _get_asset_key_for_resource(self, dbt_props: Mapping[str, Any]) -> dg.AssetKey:
        resource_type = dbt_props["resource_type"]
        meta = dbt_props.get("meta", {})
        dagster_meta = meta.get("dagster", {})

        if "asset_key" in dagster_meta and resource_type == "source":
            return dg.AssetKey(dagster_meta["asset_key"])

        # Use catalog.schema.name for Trino/Iceberg
        return dg.AssetKey(
            [
                dbt_props.get("database", "iceberg"),
                dbt_props["schema"],
                dbt_props["name"],
            ]
        )

    def _get_metadata_for_resource(self, dbt_props: Mapping[str, Any]) -> Mapping[str, Any]:
        resource_type = dbt_props["resource_type"]
        if resource_type != "model":
            return {}

        catalog = dbt_props.get("database", "iceberg")
        schema = dbt_props["schema"]
        name = dbt_props["name"]
        return {
            "trino_table": dg.MetadataValue.text(f"{catalog}.{schema}.{name}"),
        }

    def _get_automation_condition_for_resource(
        self, dbt_props: Mapping[str, Any]
    ) -> dg.AutomationCondition:
        schema = dbt_props.get("schema", "")
        if "mart" in schema.lower():
            return asset_is_new_or_updated_or_deps_updated
        return asset_is_new_or_updated

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        dbt_props = self.get_resource_props(manifest, unique_id)

        group_name = self._get_group_name_for_resource(dbt_props)
        asset_key = self._get_asset_key_for_resource(dbt_props)
        metadata = self._get_metadata_for_resource(dbt_props)
        automation_condition = self._get_automation_condition_for_resource(dbt_props)

        return base_spec.replace_attributes(
            key=asset_key,
            group_name=group_name,
            automation_condition=automation_condition,
        ).merge_attributes(metadata=metadata)


class TenantPrefixedDbtTranslator(CustomDagsterDbtTranslator):
    """Adds tenant prefix to asset keys for multi-tenant dbt models."""

    def __init__(
        self,
        settings: DagsterDbtTranslatorSettings,
        tenant_id: str,
    ):
        super().__init__(settings=settings)
        self.tenant_id = tenant_id

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        prefixed_key = base_spec.key.with_prefix(self.tenant_id)
        return base_spec.replace_attributes(key=prefixed_key)
