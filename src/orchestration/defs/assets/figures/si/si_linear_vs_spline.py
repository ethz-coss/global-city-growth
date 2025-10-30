
import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple
import numpy as np
from matplotlib.lines import Line2D

from ..figure_style import style_axes, style_config, style_inset_axes, annotate_letter_label
from ..figure_io import materialize_image, save_figure
from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_diagonal_correlation(fig, ax, df, x_axis, y_axis, title, x_axis_label, y_axis_label, diag_min, diag_max, alpha, annotation_font_size, annotation_x: float = 0.8, annotation_y: float = 0.05, is_inset: bool = False):
    diag = np.linspace(diag_min, diag_max, 100)
    ax.plot(diag, diag, color='black', linestyle='--', linewidth=2, label='Diagonal (45-degree line)')
    ax.scatter(df[x_axis], df[y_axis], color='grey', alpha=alpha)
    mean_error = np.mean(np.abs(df[y_axis] - df[x_axis]))
    ax.text(annotation_x, annotation_y, f"Mean error: {mean_error:.3f}", ha='center', va='center', transform=ax.transAxes, fontsize=annotation_font_size)
    if is_inset:
       style_inset_axes(ax, title=title, xlabel=x_axis_label, ylabel=y_axis_label)
    else:
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

def _plot_robustness_city_threshold_spline_vs_linear(fig: plt.Figure, ax_spline: plt.Axes, ax_linear: plt.Axes, df_spline_rank_size_slopes: pd.DataFrame, df_linear_rank_size_slopes: pd.DataFrame, df_spline_size_growth_slopes: pd.DataFrame, df_linear_size_growth_slopes: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    alpha = 0.2
    
    # Main plot
    x_axis = 'rank_size_slope_5k'
    y_axis = 'rank_size_slope_10k'

    x_axis_label_spline = r'$\alpha^{S} \ \text{(threshold = 5k)}$'
    x_axis_label_linear = r'$\alpha^{L} \ \text{(threshold = 5k)}$'
    y_axis_label_spline = r'$\alpha^{S} \ \text{(threshold = 10k)}$'
    y_axis_label_linear = r'$\alpha^{L} \ \text{(threshold = 10k)}$'

    df_spline_main = df_spline_rank_size_slopes
    df_linear_main = df_linear_rank_size_slopes

    annotation_font_size = style_config['label_font_size']

    diag_min, diag_max = _get_diag_bounds(df_linear=df_linear_main, df_spline=df_spline_main, x_axis=x_axis, y_axis=y_axis)
    _plot_diagonal_correlation(fig=fig, ax=ax_spline, df=df_spline_main, x_axis=x_axis, y_axis=y_axis, title=r'', x_axis_label=x_axis_label_spline, y_axis_label=y_axis_label_spline, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    _plot_diagonal_correlation(fig=fig, ax=ax_linear, df=df_linear_main, x_axis=x_axis, y_axis=y_axis, title=r'', x_axis_label=x_axis_label_linear, y_axis_label=y_axis_label_linear, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size) 

    # Inset
    inset_ax_spline = ax_spline.inset_axes([0.25, 0.75, 0.25, 0.25])
    inset_ax_linear = ax_linear.inset_axes([0.25, 0.75, 0.25, 0.25]) 

    x_axis_inset = 'size_growth_slope_5k'
    y_axis_inset = 'size_growth_slope_10k'

    x_axis_label_inset_spline = r'$\beta^{S} \ (5k)$'
    x_axis_label_inset_linear = r'$\beta^{L} \ (5k)$'
    y_axis_label_inset_spline = r'$\beta^{S} \ (10k)$'
    y_axis_label_inset_linear = r'$\beta^{L} \ (10k)$'

    df_spline_inset = df_spline_size_growth_slopes
    df_linear_inset = df_linear_size_growth_slopes

    inset_annotation_font_size = style_config['inset_label_font_size']

    diag_min_inset, diag_max_inset = _get_diag_bounds(df_linear=df_linear_inset, df_spline=df_spline_inset, x_axis=x_axis_inset, y_axis=y_axis_inset)

    _plot_diagonal_correlation(fig=fig, ax=inset_ax_spline, df=df_spline_inset, x_axis=x_axis_inset, y_axis=y_axis_inset, title=r'', x_axis_label=x_axis_label_inset_spline, y_axis_label=y_axis_label_inset_spline, diag_min=diag_min_inset, diag_max=diag_max_inset, alpha=alpha, annotation_font_size=inset_annotation_font_size, annotation_x=0.5, annotation_y=1.2, is_inset=True)
    _plot_diagonal_correlation(fig=fig, ax=inset_ax_linear, df=df_linear_inset, x_axis=x_axis_inset, y_axis=y_axis_inset, title=r'', x_axis_label=x_axis_label_inset_linear, y_axis_label=y_axis_label_inset_linear, diag_min=diag_min_inset, diag_max=diag_max_inset, alpha=alpha, annotation_font_size=inset_annotation_font_size, annotation_x=0.5, annotation_y=1.2, is_inset=True)
    


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

    _plot_diagonal_correlation(fig=fig, ax=ax_spline, df=df_spline_plot, x_axis=x_axis, y_axis=y_axis, title=r'$\leftarrow \ \text{Spline-based measurement} \ \rightarrow$', x_axis_label=x_axis_label_spline, y_axis_label=y_axis_label_spline, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    _plot_diagonal_correlation(fig=fig, ax=ax_linear, df=df_linear_plot, x_axis=x_axis, y_axis=y_axis, title=r'$\leftarrow \ \text{Linear-based measurement} \ \rightarrow$', x_axis_label=x_axis_label_linear, y_axis_label=y_axis_label_linear, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    ax_spline.legend(fontsize=annotation_font_size, frameon=False, ncol=1, bbox_to_anchor=(1, -0.17))
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

def _get_data_equation_robustness_city_threshold(postgres: PostgresResource, table_slopes_historical: str, column_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    q = f"""
    WITH slopes_10k AS (
        SELECT country, year, {column_name} AS {column_name}_10k
        FROM {table_slopes_historical}
        WHERE analysis_id = 4
    ),
    slopes_5k AS (
        SELECT country, year, {column_name} AS {column_name}_5k
        FROM {table_slopes_historical}
        WHERE analysis_id = 1
    )
    SELECT *
    FROM slopes_10k
    JOIN slopes_5k
    USING (country, year)
    """
    return pd.read_sql(q, con=postgres.get_engine())


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

def _get_data_for_equation_2(postgres: PostgresResource, table_urb_pop_share_cities_above_1m_historical: str, table_urb_pop_share_cities_above_1m_projections: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    q = f"""
    SELECT  country, 
            year, 
            analysis_id, 
            h.urban_population_share_cities_above_one_million AS m_t_data, 
            p.urban_population_share_cities_above_one_million AS m_t_proj
    FROM {table_urb_pop_share_cities_above_1m_historical} h
    JOIN {table_urb_pop_share_cities_above_1m_projections} p
    USING (analysis_id, country, year)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """ 
    return pd.read_sql(q, con=postgres.get_engine())



@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_historical(), TableNamesResource().names.world.si.world_rank_size_slopes_historical_ols(),
        TableNamesResource().names.world.figures.world_rank_size_slopes_decade_change(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical(), TableNamesResource().names.world.si.world_rank_size_slopes_ols_decade_change(), TableNamesResource().names.world.si.world_size_growth_slopes_historical_ols(), TableNamesResource().names.world.figures.world_urb_pop_share_cities_above_1m_historical(), TableNamesResource().names.world.figures.world_urb_pop_share_cities_above_1m_projections(), TableNamesResource().names.world.si.world_urb_pop_share_cities_above_1m_projections_ols()],
    group_name="si_figures"
    )
def si_figure_linear_vs_spline(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting linear vs spline figure")
    
    figure_file_name = 'si_figure_linear_vs_spline.png'
    fig, axes = plt.subplots(2, 3, figsize=(15, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.4})
    ax1, ax2, ax3, ax4, ax5, ax6 = axes.flatten()

    df_spline_rank_size_slopes = _get_data_equation_robustness_city_threshold(postgres=postgres, table_slopes_historical=tables.names.world.figures.world_rank_size_slopes_historical(), column_name='rank_size_slope')
    df_linear_rank_size_slopes = _get_data_equation_robustness_city_threshold(postgres=postgres, table_slopes_historical=tables.names.world.si.world_rank_size_slopes_historical_ols(), column_name='rank_size_slope')
    df_spline_size_growth_slopes = _get_data_equation_robustness_city_threshold(postgres=postgres, table_slopes_historical=tables.names.world.figures.world_size_growth_slopes_historical(), column_name='size_growth_slope')
    df_linear_size_growth_slopes = _get_data_equation_robustness_city_threshold(postgres=postgres, table_slopes_historical=tables.names.world.si.world_size_growth_slopes_historical_ols(), column_name='size_growth_slope')
    _plot_robustness_city_threshold_spline_vs_linear(fig=fig, ax_spline=ax1, ax_linear=ax4, df_spline_rank_size_slopes=df_spline_rank_size_slopes, df_linear_rank_size_slopes=df_linear_rank_size_slopes, df_spline_size_growth_slopes=df_spline_size_growth_slopes, df_linear_size_growth_slopes=df_linear_size_growth_slopes)

    df_spline = _get_data_for_equation_1(postgres=postgres, table_rank_size_slopes_decade_change=tables.names.world.figures.world_rank_size_slopes_decade_change(), table_size_growth_slopes=tables.names.world.figures.world_size_growth_slopes_historical())
    df_linear = _get_data_for_equation_1(postgres=postgres, table_rank_size_slopes_decade_change=tables.names.world.si.world_rank_size_slopes_ols_decade_change(), table_size_growth_slopes=tables.names.world.si.world_size_growth_slopes_historical_ols())
    _plot_eq1_spline_vs_linear(fig=fig, ax_spline=ax2, ax_linear=ax5, df_spline=df_spline, df_linear=df_linear)

    df_spline = _get_data_for_equation_2(postgres=postgres, table_urb_pop_share_cities_above_1m_historical=tables.names.world.figures.world_urb_pop_share_cities_above_1m_historical(), table_urb_pop_share_cities_above_1m_projections=tables.names.world.figures.world_urb_pop_share_cities_above_1m_projections())
    df_linear = _get_data_for_equation_2(postgres=postgres, table_urb_pop_share_cities_above_1m_historical=tables.names.world.figures.world_urb_pop_share_cities_above_1m_historical(), table_urb_pop_share_cities_above_1m_projections=tables.names.world.si.world_urb_pop_share_cities_above_1m_projections_ols())
    _plot_eq2_spline_vs_linear(fig=fig, ax_spline=ax3, ax_linear=ax6, df_spline=df_spline, df_linear=df_linear)

    y_fig = 0.485
    line = Line2D([0.1, 0.4], [y_fig, y_fig],transform=fig.transFigure, color='0.6', lw=1, ls='--', zorder=1000, clip_on=False)
    fig.add_artist(line)
    line = Line2D([0.65, 0.9], [y_fig, y_fig],transform=fig.transFigure, color='0.6', lw=1, ls='--', zorder=1000, clip_on=False)
    fig.add_artist(line)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5, ax6], left_side=[False, True, True, False, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)
    return materialize_image(figure_file_name=figure_file_name, si=True)