import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..resources.resources import dbt_project, PostgresResource
from .constants import constants

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        name = dbt_resource_props["name"]
        return dg.AssetKey(name)
        
    def get_group_name(self, dbt_resource_props):
        gname = "_".join(dbt_resource_props["fqn"][1:-1])
        return gname


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude="incremental_example_table usa_cluster_base_geom"
)
def dbt_warehouse(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    partitions_def=dg.StaticPartitionsDefinition([str(s) for s in constants['USA_PIXEL_THRESHOLDS']]),
    select="usa_cluster_base_geom",
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    pool="duckdb_write"
)
def usa_cluster_base_geom(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    pixel_threshold = context.partition_key
    context.log.info(f"pixel_threshold: {pixel_threshold}")
    dbt_vars = {
        "pixel_threshold": pixel_threshold
    }
    args = [ "run", "--vars", json.dumps(dbt_vars) ]
    yield from dbt.cli(args, context=context).stream()

## A DUMMY EXAMPLE OF AN INCREMENTAL TABLE TO BE REMOVED LATER
# TODO: REMOVE THIS LATER
import pandas as pd

@dg.asset(
    group_name="usa_intermediate_incremental_example",
    kinds={"postgres"},
)
def my_example_asset(context: dg.AssetExecutionContext, postgres: PostgresResource):
    my_table = [['id1', 1, 2, 3], ['id2', 4, 5, 6], ['id3', 7, 8, 9]]
    my_table = pd.DataFrame(my_table, columns=["id", "a", "b", "c"])

    engine = postgres.get_engine()
    my_table.to_sql(name="my_example_asset", con=engine, if_exists="replace", index=False)

my_vars = [1, 2, 3]
partition_my_vars = dg.StaticPartitionsDefinition([str(s) for s in my_vars])
import json

# TODO: REMOVE THIS LATER AND REMEBER TO REMOVE THE TABLE IN THE DBT PROJECT: src/warehouse/models/usa/intermediate/incremental_example/incremental_example_table.sql
@dbt_assets(
    manifest=dbt_project.manifest_path,
    partitions_def=partition_my_vars,
    select="incremental_example_table",
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def incremental_example_table(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    my_var_value = context.partition_key
    context.log.info(f"my_var_value: {my_var_value}")
    dbt_vars = {
        "my_var": my_var_value
    }
    args = [ "run", "--vars", json.dumps(dbt_vars) ]
    yield from dbt.cli(args, context=context).stream()
   