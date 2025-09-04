import dagster as dg
import pandas as pd
from pygam import LinearGAM, s
import numpy as np
from typing import List

from ..resources.resources import PostgresResource, StorageResource, TableNamesResource
from .constants import constants


def fit_penalized_b_spline(df, xaxis, yaxis, lam):
    X = df[[xaxis]].values
    y = df[yaxis].values
    gam = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X, y)
    
    grid = gam.generate_X_grid(term=0)
    y_pdep, ci = gam.partial_dependence(term=0, X=grid, width=0.95)
    x = grid[:, 0]
    return x, y_pdep, ci


def get_mean_derivative_penalized_b_spline(df, xaxis, yaxis, lam):
    x, y, ci = fit_penalized_b_spline(df=df, xaxis=xaxis, yaxis=yaxis, lam=lam)
    derivative = np.gradient(y, x)
    mean_derivative = np.mean(derivative)
    return mean_derivative

def get_slopes_for_all_analysis_ids(df: pd.DataFrame, analysis_ids: List[int], group_by_keys: List[str], slopes_column_name: str, xaxis: str, yaxis: str, lam: float) -> pd.DataFrame:
    slopes = []
    for analysis_id in analysis_ids:
        df_analysis_id = df[df['analysis_id'] == analysis_id]
        slopes_analysis_id = df_analysis_id.groupby(group_by_keys).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=xaxis, yaxis=yaxis, lam=lam)).reset_index().rename(columns={0: slopes_column_name})
        slopes_analysis_id['analysis_id'] = analysis_id
        slopes.append(slopes_analysis_id)

    slopes = pd.concat(slopes)
    return slopes


@dg.asset(
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def analysis_parameters(context: dg.AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    analysis_parameters_path = storage.paths.other.analysis_parameters()
    context.log.info(f"Copying countries with region and subregion from {analysis_parameters_path}")
    analysis_parameters_df = pd.read_csv(analysis_parameters_path)
    return analysis_parameters_df

@dg.asset(
    deps=["world_size_vs_growth", "analysis_parameters"],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_size_growth_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource)  -> pd.DataFrame:
    context.log.info("Calculating world size growth slopes LALLALALA")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    slopes_column_name = 'size_growth_slope'

    world_size_vs_growth =pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()}", con=postgres.get_engine())
    analysis_parameters = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = analysis_parameters['analysis_id'].tolist()

    world_size_growth_slopes_df = get_slopes_for_all_analysis_ids(df=world_size_vs_growth, analysis_ids=analysis_ids, group_by_keys=['country', 'year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    return world_size_growth_slopes_df


@dg.asset(
    deps=["world_rank_vs_size", "analysis_parameters"],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world rank size slopes AAAAA")
    xaxis = 'log_rank'
    yaxis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    slopes_column_name = 'rank_size_slope'

    world_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()}", con=postgres.get_engine())
    analysis_parameters = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = analysis_parameters['analysis_id'].tolist()

    world_rank_size_slopes_df = get_slopes_for_all_analysis_ids(df=world_rank_vs_size, analysis_ids=analysis_ids, group_by_keys=['country', 'year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    world_rank_size_slopes_df[slopes_column_name] = world_rank_size_slopes_df[slopes_column_name].abs()
    return world_rank_size_slopes_df


@dg.asset(
    deps=["usa_size_vs_growth", "analysis_parameters"],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def usa_size_growth_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating usa size growth slopes AAAAA")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    slopes_column_name = 'size_growth_slope'

    usa_size_vs_growth = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_size_vs_growth()}", con=postgres.get_engine())
    analysis_parameters = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = analysis_parameters['analysis_id'].tolist()

    usa_size_growth_slopes_df = get_slopes_for_all_analysis_ids(df=usa_size_vs_growth, analysis_ids=analysis_ids, group_by_keys=['year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    usa_size_growth_slopes_df[slopes_column_name] = usa_size_growth_slopes_df[slopes_column_name].abs()
    return usa_size_growth_slopes_df


@dg.asset(
    deps=["usa_rank_vs_size", "analysis_parameters"],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def usa_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating usa rank size slopes AAAAA")
    xaxis = 'log_rank'
    yaxis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    slopes_column_name = 'rank_size_slope'

    usa_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_rank_vs_size()}", con=postgres.get_engine())
    analysis_parameters = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = analysis_parameters['analysis_id'].tolist()

    usa_rank_size_slopes_df = get_slopes_for_all_analysis_ids(df=usa_rank_vs_size, analysis_ids=analysis_ids, group_by_keys=['year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    usa_rank_size_slopes_df[slopes_column_name] = usa_rank_size_slopes_df[slopes_column_name].abs()
    return usa_rank_size_slopes_df
