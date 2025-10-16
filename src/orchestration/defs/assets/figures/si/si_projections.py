import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple
import numpy as np
import seaborn as sns

from ..figure_style import style_axes, style_config, annotate_letter_label
from ..figure_io import materialize_image, save_figure
from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_diagonal_correlation(fig, ax, df, x_axis, y_axis, title, x_axis_label, y_axis_label, diag_min, diag_max, alpha, annotation_font_size):
    diag = np.linspace(diag_min, diag_max, 100)
    ax.plot(diag, diag, color='black', linestyle='--', linewidth=2, label='Diagonal (45-degree line)')
    ax.scatter(df[x_axis], df[y_axis], color='grey', alpha=alpha)
    mean_error = np.mean(np.abs(df[y_axis] - df[x_axis]))
    ax.text(0.8, 0.05, f"Mean error: {mean_error:.3f}", ha='center', va='center', transform=ax.transAxes, fontsize=annotation_font_size)
    style_axes(ax, title=title, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax

def _filter_data_by_quantiles(df: pd.DataFrame, x_axis: str, y_axis: str, quantile_min: float, quantile_max: float) -> pd.DataFrame:
    x_value_quantile_min = df[x_axis].quantile(quantile_min)
    x_value_quantile_max = df[x_axis].quantile(quantile_max)
    y_value_quantile_min = df[y_axis].quantile(quantile_min)
    y_value_quantile_max = df[y_axis].quantile(quantile_max)
    return df[df[x_axis].between(x_value_quantile_min, x_value_quantile_max) & df[y_axis].between(y_value_quantile_min, y_value_quantile_max)]

def _get_diag_bounds(df_linear: pd.DataFrame, df_spline: pd.DataFrame, x_axis: str, y_axis: str) -> Tuple[float, float]:
    diag_min_spline = min(df_spline[x_axis].min(), df_spline[y_axis].min())
    diag_max_spline = max(df_spline[x_axis].max(), df_spline[y_axis].max())
    diag_min_linear = min(df_linear[x_axis].min(), df_linear[y_axis].min())
    diag_max_linear = max(df_linear[x_axis].max(), df_linear[y_axis].max())
    diag_min = min(diag_min_spline, diag_min_linear)
    diag_max = max(diag_max_spline, diag_max_linear)
    return diag_min, diag_max

def _plot_eq1_spline_vs_linear(fig: plt.Figure, ax_spline: plt.Axes, ax_linear: plt.Axes, df_spline: pd.DataFrame, df_linear: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    q_min, q_max = 0.01, 0.99

    x_axis = 'size_growth_slope'
    y_axis = 'rank_size_slope_decade_change'

    x_axis_label_spline = r'$1 + \beta^{S}_{t}$'
    x_axis_label_linear = r'$1 + \beta^{L}_{t}$'
    y_axis_label_spline = r'$\alpha^{S}_{t+10} \ / \ \alpha^{S}_{t}$'
    y_axis_label_linear = r'$\alpha^{L}_{t+10} \ / \ \alpha^{L}_{t}$'

    alpha = 0.2

    annotation_font_size = style_config['label_font_size']

    df_spline_plot = _filter_data_by_quantiles(df=df_spline, x_axis=x_axis, y_axis=y_axis, quantile_min=q_min, quantile_max=q_max) 
    df_linear_plot = _filter_data_by_quantiles(df=df_linear, x_axis=x_axis, y_axis=y_axis, quantile_min=q_min, quantile_max=q_max)
    diag_min, diag_max = _get_diag_bounds(df_linear=df_linear_plot, df_spline=df_spline_plot, x_axis=x_axis, y_axis=y_axis)

    _plot_diagonal_correlation(fig=fig, ax=ax_spline, df=df_spline_plot, x_axis=x_axis, y_axis=y_axis, title='Spline-based measurement', x_axis_label=x_axis_label_spline, y_axis_label=y_axis_label_spline, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    _plot_diagonal_correlation(fig=fig, ax=ax_linear, df=df_linear_plot, x_axis=x_axis, y_axis=y_axis, title='Linear-based measurement', x_axis_label=x_axis_label_linear, y_axis_label=y_axis_label_linear, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    ax_spline.legend(fontsize=annotation_font_size, frameon=False, ncol=1, bbox_to_anchor=(0.825, 0.925))
    return fig, ax_spline, ax_linear


def _plot_eq2_spline_vs_linear(fig: plt.Figure, ax_spline: plt.Axes, ax_linear: plt.Axes, df_spline: pd.DataFrame, df_linear: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'm_t_data'
    y_axis = 'm_t_proj'

    x_axis_label = r'$\hat{m_t}$'
    y_axis_label_spline = r'$m^{S}_t$'
    y_axis_label_linear = r'$m^{L}_t$'

    alpha = 0.2

    annotation_font_size = style_config['label_font_size']

    diag_min, diag_max = 0, 1
    _plot_diagonal_correlation(fig=fig, ax=ax_spline, df=df_spline, x_axis=x_axis, y_axis=y_axis, title='', x_axis_label=x_axis_label, y_axis_label=y_axis_label_spline, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size )
    _plot_diagonal_correlation(fig=fig, ax=ax_linear, df=df_linear, x_axis=x_axis, y_axis=y_axis, title='', x_axis_label=x_axis_label, y_axis_label=y_axis_label_linear, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    return fig, ax_spline, ax_linear

def _plot_projection_vs_historical_share_population_cities_above_1m(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, is_country_plot: bool) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'm_t_data'
    y_axis = 'm_t_proj'

    y_axis_label = r'$m_t$'
    x_axis_label = r'$\hat{m_t}$'

    alpha = 0.4

    if is_country_plot:
        title = 'Country'
    else:
        title = 'Region'

    annotation_font_size = style_config['label_font_size']

    diag_min, diag_max = 0, 1
    _plot_diagonal_correlation(fig=fig, ax=ax, df=df, x_axis=x_axis, y_axis=y_axis, title=title, x_axis_label=x_axis_label, y_axis_label=y_axis_label, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    if is_country_plot:
        ax.legend(fontsize=annotation_font_size, frameon=False, ncol=1, bbox_to_anchor=(0.83, 0.95))
    return fig, ax


def _get_data_for_equation_1(postgres: PostgresResource, table_rank_size_slopes_decade_change: str, table_size_growth_slopes: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    q = f"""
    SELECT  country, 
            year, 
            rank_size_slope_decade_change + 1 AS rank_size_slope_decade_change, 
            size_growth_slope + 1 AS size_growth_slope
    FROM {table_rank_size_slopes_decade_change}
    JOIN {table_size_growth_slopes}
    USING (analysis_id, country, year)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """
    return pd.read_sql(q, con=postgres.get_engine())

def _get_data_for_equation_2(postgres: PostgresResource, table_world_population_share_cities_above_1m_historical: str, table_world_population_share_cities_above_1m_projections: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    q = f"""
    SELECT  country, 
            year, 
            analysis_id, 
            h.urban_population_share_cities_above_one_million AS m_t_data, 
            p.urban_population_share_cities_above_one_million AS m_t_proj
    FROM {table_world_population_share_cities_above_1m_historical} h
    JOIN {table_world_population_share_cities_above_1m_projections} p
    USING (analysis_id, country, year)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """ 
    return pd.read_sql(q, con=postgres.get_engine())


def _get_data_for_projection_vs_historical_share_population_cities_above_1m(postgres: PostgresResource, table_world_population_share_cities_above_1m_historical: str, table_world_population_share_cities_above_1m_projections: str, table_world_urban_population: str, table_world_regions: str) -> pd.DataFrame:
    q = f"""
    SELECT  country, 
            year, 
            analysis_id, 
            u.urban_population,
            r.region2 AS region,
            h.urban_population_share_cities_above_one_million AS m_t_data, 
            p.urban_population_share_cities_above_one_million AS m_t_proj
    FROM {table_world_population_share_cities_above_1m_historical} h
    JOIN {table_world_population_share_cities_above_1m_projections} p
    USING (analysis_id, country, year)
    JOIN {table_world_urban_population} u
    USING (country, year)
    JOIN {table_world_regions} r
    USING (country)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """ 
    df_country = pd.read_sql(q, con=postgres.get_engine())
    
    def estimator_func(col_name):
        return lambda x: np.sum(x[col_name] * x['urban_population']) / np.sum(x['urban_population'])
    df_region_data = df_country.groupby(['region', 'year']).apply(estimator_func('m_t_data')).reset_index().rename(columns={0: 'm_t_data'})
    df_region_proj = df_country.groupby(['region', 'year']).apply(estimator_func('m_t_proj')).reset_index().rename(columns={0: 'm_t_proj'})
    df_region = pd.merge(df_region_data, df_region_proj, on=['region', 'year'], how='inner')
    return df_country, df_region   

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_decade_change(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical(), TableNamesResource().names.world.si.world_rank_size_slopes_ols_decade_change(), TableNamesResource().names.world.si.world_size_growth_slopes_historical_ols(), TableNamesResource().names.world.figures.world_population_share_cities_above_1m_historical(), TableNamesResource().names.world.figures.world_population_share_cities_above_1m_projections(), TableNamesResource().names.world.si.world_population_share_cities_above_1m_projections_ols()],
    group_name="si_figures"
)
def si_figure_equation_correlation(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting equation correlation figure")
    figure_file_name = 'si_figure_equation_correlation.png'
    fig, axes = plt.subplots(2, 2, figsize=(10, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.25})
    ax1, ax2, ax3, ax4 = axes.flatten()

    df_spline = _get_data_for_equation_1(postgres=postgres, table_rank_size_slopes_decade_change=tables.names.world.figures.world_rank_size_slopes_decade_change(), table_size_growth_slopes=tables.names.world.figures.world_size_growth_slopes_historical())
    df_linear = _get_data_for_equation_1(postgres=postgres, table_rank_size_slopes_decade_change=tables.names.world.si.world_rank_size_slopes_ols_decade_change(), table_size_growth_slopes=tables.names.world.si.world_size_growth_slopes_historical_ols())
    _plot_eq1_spline_vs_linear(fig=fig, ax_spline=ax1, ax_linear=ax2, df_spline=df_spline, df_linear=df_linear)

    df_spline = _get_data_for_equation_2(postgres=postgres, table_world_population_share_cities_above_1m_historical=tables.names.world.figures.world_population_share_cities_above_1m_historical(), table_world_population_share_cities_above_1m_projections=tables.names.world.figures.world_population_share_cities_above_1m_projections())
    df_linear = _get_data_for_equation_2(postgres=postgres, table_world_population_share_cities_above_1m_historical=tables.names.world.figures.world_population_share_cities_above_1m_historical(), table_world_population_share_cities_above_1m_projections=tables.names.world.si.world_population_share_cities_above_1m_projections_ols())
    _plot_eq2_spline_vs_linear(fig=fig, ax_spline=ax3, ax_linear=ax4, df_spline=df_spline, df_linear=df_linear)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4], left_side=[True, True, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)

    return materialize_image(figure_file_name=figure_file_name, si=True)


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_population_share_cities_above_1m_historical(), TableNamesResource().names.world.figures.world_population_share_cities_above_1m_projections(), TableNamesResource().names.world.si.world_urban_population(), TableNamesResource().names.world.sources.world_country_region()],
    group_name="si_figures"
)
def si_figure_projection_vs_historical_share_population_cities_above_1m(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting projection vs historical share population cities above 1m figure")

    figure_file_name = 'si_figure_projection_vs_historical_share_population_cities_above_1m.png'
    fig, axes = plt.subplots(1, 2, figsize=(10, 5), gridspec_kw={'wspace': 0.25})
    ax1, ax2 = axes.flatten()
    
    df_country, df_region = _get_data_for_projection_vs_historical_share_population_cities_above_1m(postgres=postgres, table_world_population_share_cities_above_1m_historical=tables.names.world.figures.world_population_share_cities_above_1m_historical(), table_world_population_share_cities_above_1m_projections=tables.names.world.figures.world_population_share_cities_above_1m_projections(), table_world_urban_population=tables.names.world.si.world_urban_population(), table_world_regions=tables.names.world.sources.world_country_region())

    _plot_projection_vs_historical_share_population_cities_above_1m(fig=fig, ax=ax1, df=df_country, is_country_plot=True)
    _plot_projection_vs_historical_share_population_cities_above_1m(fig=fig, ax=ax2, df=df_region, is_country_plot=False)

    annotate_letter_label(axes=[ax1, ax2], left_side=[True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)
    return materialize_image(figure_file_name=figure_file_name, si=True)

