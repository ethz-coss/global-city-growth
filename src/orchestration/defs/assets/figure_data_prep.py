import dagster as dg
import pandas as pd

from ..resources.resources import PostgresResource, StorageResource, TableNamesResource

@dg.asset(
    kinds={'postgres'},
    group_name="figure_data_prep"
)
def analysis_parameters(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    analysis_parameters_path = storage.paths.other.analysis_parameters()
    context.log.info(f"Copying countries with region and subregion from {analysis_parameters_path}")

    analysis_parameters_df = pd.read_csv(analysis_parameters_path)
    analysis_parameters_df.to_sql(
        name=tables.names.other.analysis_parameters(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )