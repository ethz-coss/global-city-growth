import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..resources.resources import dbt_project, PostgresResource
from .constants import constants

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        name = dbt_resource_props["name"]
        return dg.AssetKey(name)
        
    def get_group_name(self, dbt_resource_props):
        if "analysis_parameters" in dbt_resource_props["fqn"]:
            return "figure_data_prep"
        if "world_paper_stats" in dbt_resource_props["fqn"]:
            return "paper_stats"
        
        gname = "_".join(dbt_resource_props["fqn"][1:-1])
        if 'figures' in gname and 'si_figures' not in gname:
            gname = "figure_data_prep"
        elif 'si_figures' in gname:
            gname = "si_figure_data_prep"

        return gname


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_warehouse(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()