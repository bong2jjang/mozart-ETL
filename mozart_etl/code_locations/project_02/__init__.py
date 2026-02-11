"""Code location for project_02 tenant — fully isolated."""

from pathlib import Path

from mozart_etl.code_locations._tenant_factory import create_tenant_defs

defs = create_tenant_defs(Path(__file__).parent / "tenant.yaml")
