import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..resources.resources import dbt_project

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        name = dbt_resource_props["name"]
        return dg.AssetKey(name)
        
    def get_group_name(self, dbt_resource_props):
        gname = "_".join(dbt_resource_props["fqn"][1:-1])
        return gname


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_warehouse(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

