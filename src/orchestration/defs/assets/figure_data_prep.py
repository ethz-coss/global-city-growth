import dagster as dg
import pandas as pd
from pygam import LinearGAM, s
import numpy as np

from ..resources.resources import PostgresResource, StorageResource, TableNamesResource
from .constants import constants


analysis_ids_partition = dg.DynamicPartitionsDefinition(name="analysis_ids")


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

    analysis_ids = analysis_parameters_df['analysis_id'].tolist()
    context.instance.add_dynamic_partitions(partitions_def_name=analysis_ids_partition.name, partition_keys=analysis_ids)
   

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


@dg.asset(
    deps=["world_size_vs_growth"],
    kinds={'postgres'},
    partitions_def=analysis_ids_partition,
    group_name="figure_data_prep"
)
def world_size_growth_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info("Preparing figure data")
    analysis_id = context.partition_key

    q = f"""
    SELECT * 
    FROM {tables.names.world.figures.world_size_vs_growth()}
    WHERE analysis_id = {analysis_id}
    """
    world_size_vs_growth = pd.read_sql(sql=q, con=postgres.get_engine())

    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    world_size_growth_slopes = world_size_vs_growth.groupby(['country', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=xaxis, yaxis=yaxis, lam=lam)).reset_index().rename(columns={0: 'size_growth_slope'})
    world_size_growth_slopes['analysis_id'] = analysis_id
    world_size_growth_slopes.to_sql(
        name=tables.names.world.figures.world_size_growth_slopes(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )


@dg.asset(
    deps=["world_rank_vs_size"],
    kinds={'postgres'},
    partitions_def=analysis_ids_partition,
    group_name="figure_data_prep"
)
def world_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info("Preparing figure data")
    analysis_id = context.partition_key

    q = f"""
    SELECT * 
    FROM {tables.names.world.figures.world_rank_vs_size()}
    WHERE analysis_id = {analysis_id}
    """
    world_rank_vs_size = pd.read_sql(sql=q, con=postgres.get_engine())

    xaxis = 'log_rank'
    yaxis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']

    world_rank_size_slopes = world_rank_vs_size.groupby(['country', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=xaxis, yaxis=yaxis, lam=lam)).reset_index().rename(columns={0: 'rank_size_slope'})
    world_rank_size_slopes['rank_size_slope'] = world_rank_size_slopes['rank_size_slope'].abs()
    world_rank_size_slopes['analysis_id'] = analysis_id
    world_rank_size_slopes.to_sql(
        name=tables.names.world.figures.world_rank_size_slopes(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )