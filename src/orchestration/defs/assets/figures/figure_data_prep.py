# src/orchestration/defs/assets/figures/figure_data_prep.py
import dagster as dg
import pandas as pd
from pygam import LinearGAM, s
import numpy as np
from typing import List
import statsmodels.formula.api as smf

from ...resources.resources import PostgresResource, StorageResource, TableNamesResource
from ..constants import constants
from .figure_stats import get_mean_derivative_penalized_b_spline, get_ols_slope

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_size_growth_slopes_historical(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource)  -> pd.DataFrame:
    context.log.info("Calculating world size growth slopes")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    world_size_vs_growth = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()}", con=postgres.get_engine())
    slopes = world_size_vs_growth.groupby(['analysis_id', 'country', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=xaxis, yaxis=yaxis, lam=lam)).reset_index().rename(columns={0:'size_growth_slope'})
    return slopes

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_rank_size_slopes_historical(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world rank size slopes")
    xaxis = 'log_rank'
    yaxis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']

    world_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()}", con=postgres.get_engine())
    slopes = world_rank_vs_size.groupby(['analysis_id', 'country', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=xaxis, yaxis=yaxis, lam=lam)).reset_index().rename(columns={0:'rank_size_slope'})
    slopes['rank_size_slope'] = slopes['rank_size_slope'].abs()
    return slopes


def _get_projections_for_world_size_growth_slopes(df_size_growth_slopes: pd.DataFrame, df_urbanization_projections: pd.DataFrame) -> pd.DataFrame:
    size_growth_reg = smf.ols('size_growth_slope ~ urban_population_share + C(country) - 1', data=df_size_growth_slopes).fit()
    countries_analysis_id = df_size_growth_slopes['country'].unique()
    df_urbanization_projections_analysis_id = df_urbanization_projections[df_urbanization_projections['country'].isin(countries_analysis_id)].copy()
    predictions = size_growth_reg.predict(df_urbanization_projections_analysis_id)
    return df_urbanization_projections_analysis_id.assign(size_growth_slope=predictions)[['country', 'year', 'size_growth_slope']]

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(), TableNamesResource().names.world.figures.world_urbanization()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_size_growth_slopes_projections(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world size growth slopes projections")
    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_urbanization()}
    WHERE year >= 2020 AND MOD(year, 5) = 0
    """
    urbanization_projections = pd.read_sql(q, con=postgres.get_engine())

    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_size_growth_slopes_historical_urbanization()}
    WHERE year < 2020
    """
    size_growth_slopes = pd.read_sql(q, con=postgres.get_engine())
    size_growth_slopes_projections = size_growth_slopes.groupby('analysis_id', group_keys=False).apply(lambda g: _get_projections_for_world_size_growth_slopes(df_size_growth_slopes=g,df_urbanization_projections=urbanization_projections).assign(analysis_id=g.name)).reset_index(drop=True)
    return size_growth_slopes_projections


def _get_regression_results_for_region_regression_with_urbanization_controls(df: pd.DataFrame) -> pd.DataFrame:
    x_axis, y_axis, region_col = 'urban_population_share', 'size_growth_slope', 'region2'
    x_c, y_c = f'{x_axis}_centered', f'{y_axis}_centered'

    d = df.assign(
        **{
            y_c: df[y_axis] - df[y_axis].mean(),
            x_c: df[x_axis] - df[x_axis].mean(),
        }
    )

    reg_nc = smf.ols(f'{y_c} ~ C({region_col}) - 1', data=d).fit()
    reg_c  = smf.ols(f'{y_c} ~ C({region_col}) + {x_c} - 1', data=d).fit()

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
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(), TableNamesResource().names.world.sources.world_country_region()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def world_region_regression_with_urbanization_controls(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating world region regression with urbanization controls")
    
    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_size_growth_slopes_historical_urbanization()}
    JOIN {tables.names.world.sources.world_country_region()}
    USING (country)
    WHERE year < 2020
    """ 
    size_growth_slopes_urbanization_region = pd.read_sql(q, con=postgres.get_engine())
    results = size_growth_slopes_urbanization_region.groupby('analysis_id', group_keys=False).apply(lambda g: _get_regression_results_for_region_regression_with_urbanization_controls(df=g).assign(analysis_id=g.name)).reset_index(drop=True)
    return results


@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_rank_vs_size()],
    kinds={'postgres'},
    group_name="figure_data_prep",
    io_manager_key="postgres_io_manager"
)
def usa_rank_size_slopes(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> pd.DataFrame:
    context.log.info("Calculating usa rank size slopes urbanization")
    x_axis = 'log_rank'
    y_axis = 'log_population'
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    
    usa_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_rank_vs_size()}", con=postgres.get_engine())
    slopes = usa_rank_vs_size.groupby(['analysis_id', 'year']).apply(lambda x: get_mean_derivative_penalized_b_spline(df=x, xaxis=x_axis, yaxis=y_axis, lam=lam)).reset_index().rename(columns={0:'rank_size_slope'})
    slopes['rank_size_slope'] = slopes['rank_size_slope'].abs()
    return slopes