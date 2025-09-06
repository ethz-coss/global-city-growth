# src/orchestration/defs/assets/figures/tables.py
import dagster as dg
import pandas as pd
import os
from typing import Tuple


from ....resources.resources import PostgresResource, TableNamesResource
from ..figure_config import table_si_dir, MAIN_ANALYSIS_ID
from ..tables import make_table_2


def get_analysis_ids(postgres: PostgresResource, tables: TableNamesResource) -> Tuple[str, str]:
    analysis_ids = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = analysis_ids['analysis_id'].tolist()
    return analysis_ids



@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_urbanization(), TableNamesResource().names.world.figures.world_rank_size_slopes_urbanization(), TableNamesResource().names.other.analysis_parameters()],
    group_name="supplementary_information"
)
def world_robustness_tables(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Creating world robustness tables")
    analysis_ids = get_analysis_ids(postgres=postgres, tables=tables)

    for analysis_id in analysis_ids:
        if analysis_id == MAIN_ANALYSIS_ID:
            continue

        table_file_name = f'table_2_robustness_{analysis_id}.txt'
        table_path = os.path.join(table_si_dir, table_file_name)

        context.log.info(f"Creating table for analysis id {analysis_id}")
        world_size_growth_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_growth_slopes_urbanization()} WHERE analysis_id = {analysis_id}", con=postgres.get_engine())
        world_rank_size_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_size_slopes_urbanization()} WHERE analysis_id = {analysis_id}", con=postgres.get_engine())  
        latex_table = make_table_2(df_size_growth_slopes=world_size_growth_slopes_urbanization, df_rank_size_slopes=world_rank_size_slopes_urbanization)

        with open(table_path, 'w') as f:
            f.write(latex_table)

    return dg.MaterializeResult(
        metadata={
            "path": str(table_si_dir),
            "num_records_processed": len(analysis_ids),
        }
    )