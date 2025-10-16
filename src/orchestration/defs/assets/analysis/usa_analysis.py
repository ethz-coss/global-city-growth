# src/orchestration/defs/assets/figures/figure_data_prep.py
import dagster as dg
import pandas as pd

from ...resources.resources import PostgresResource, TableNamesResource
from ..constants import constants
from ..stats_utils import get_mean_derivative_penalized_b_spline



@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_rank_vs_size()],
    kinds={'postgres'},
    group_name="usa_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="string", description="see usa_cluster_growth_population_analysis"),
            dg.TableColumn(name="year", type="int"),
            dg.TableColumn(name="rank_size_slope", type="float", description="The average slope of the log-rank vs log-size curve fitted using a penalized B-spline"),
        ])
    }
)
def usa_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    """
    We take the log-rank and log-size of the cities in a given year and fit a curve through it using a penalized B-spline. We then compute the average slope of the curve.
    """
    context.log.info("Calculating usa rank size slopes urbanization")
    x_axis = 'log_rank'
    y_axis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    
    usa_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_rank_vs_size()}", con=postgres.get_engine())
    slopes = usa_rank_vs_size.groupby(['analysis_id', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=x_axis, yaxis=y_axis, lam=lam)).reset_index().rename(columns={0:'rank_size_slope'})
    slopes['rank_size_slope'] = slopes['rank_size_slope'].abs()
    return slopes