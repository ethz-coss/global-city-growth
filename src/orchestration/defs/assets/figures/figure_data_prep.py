# src/orchestration/defs/assets/figures/figure_data_prep.py
import dagster as dg
import pandas as pd
from pygam import LinearGAM, s
import numpy as np
from typing import List
import statsmodels.formula.api as smf

from ...resources.resources import PostgresResource, StorageResource, TableNamesResource
from ..constants import constants
from .figure_utils import get_mean_derivative_penalized_b_spline
from .figure_config import MAIN_ANALYSIS_ID

def _get_slopes_for_all_analysis_ids(df: pd.DataFrame, analysis_ids: List[int], group_by_keys: List[str], slopes_column_name: str, xaxis: str, yaxis: str, lam: float) -> pd.DataFrame:
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
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_size_growth_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource)  -> pd.DataFrame:
    context.log.info("Calculating world size growth slopes")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    slopes_column_name = 'size_growth_slope'

    world_size_vs_growth =pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()}", con=postgres.get_engine())
    analysis_ids = world_size_vs_growth['analysis_id'].unique().tolist()

    world_size_growth_slopes_df = _get_slopes_for_all_analysis_ids(df=world_size_vs_growth, analysis_ids=analysis_ids, group_by_keys=['country', 'year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    return world_size_growth_slopes_df


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world rank size slopes")
    xaxis = 'log_rank'
    yaxis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    slopes_column_name = 'rank_size_slope'

    world_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()}", con=postgres.get_engine())
    analysis_ids = world_rank_vs_size['analysis_id'].unique().tolist()

    world_rank_size_slopes_df = _get_slopes_for_all_analysis_ids(df=world_rank_vs_size, analysis_ids=analysis_ids, group_by_keys=['country', 'year'], slopes_column_name=slopes_column_name, xaxis=xaxis, yaxis=yaxis, lam=lam)
    world_rank_size_slopes_df[slopes_column_name] = world_rank_size_slopes_df[slopes_column_name].abs()
    return world_rank_size_slopes_df


def _get_regression_results_for_region_regression_with_urbanization_controls(df: pd.DataFrame) -> pd.DataFrame:
    x_axis = 'urban_population_share'
    y_axis = 'size_growth_slope'
    region_col = 'region2'

    x_axis_centered = f'{x_axis}_centered'
    y_axis_centered = f'{y_axis}_centered'


    df[y_axis_centered] = df[y_axis] - df[y_axis].mean()
    df[x_axis_centered] = df[x_axis] - df[x_axis].mean()

    reg_nc = smf.ols(f'{y_axis_centered} ~ C({region_col}) - 1', data=df).fit()
    reg_c = smf.ols(f'{y_axis_centered} ~ C({region_col}) + {x_axis_centered} - 1', data=df).fit()

    regions = sorted(df[region_col].unique())

    res = []
    for r in regions:
        c_nc = reg_nc.params[f'C({region_col})[{r}]']
        c_nc_low, c_nc_high = reg_nc.conf_int().loc[f'C({region_col})[{r}]']
        c_c = reg_c.params[f'C({region_col})[{r}]']
        c_c_low, c_c_high = reg_c.conf_int().loc[f'C({region_col})[{r}]']
        res.append({
            'region': r,
            'coeff_no_control': c_nc,
            'coeff_with_control': c_c,
            'ci_low_no_control': c_nc_low,
            'ci_high_no_control': c_nc_high,
            'ci_low_with_control': c_c_low,
            'ci_high_with_control': c_c_high
        })

    res = pd.DataFrame(res)
    res = res.sort_values(by='coeff_no_control', ascending=False)
    return res


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_urbanization(), TableNamesResource().names.world.sources.world_country_region()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_region_regression_with_urbanization_controls(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world region regression with urbanization controls")
    
    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_size_growth_slopes_urbanization()}
    JOIN {tables.names.world.sources.world_country_region()}
    USING (country)
    WHERE year < 2020
    """ 
    size_growth_slopes_urbanization_region = pd.read_sql(q, con=postgres.get_engine())
    analysis_ids = size_growth_slopes_urbanization_region['analysis_id'].unique().tolist()

    results = []
    for analysis_id in analysis_ids:
        size_growth_slopes_urbanization_region_analysis_id = size_growth_slopes_urbanization_region[size_growth_slopes_urbanization_region['analysis_id'] == analysis_id]
        res = _get_regression_results_for_region_regression_with_urbanization_controls(df=size_growth_slopes_urbanization_region_analysis_id)
        res['analysis_id'] = analysis_id
        results.append(res)
    
    results = pd.concat(results)
    return results

    