import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import os
from typing import Tuple, Dict, Any

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_config import style_config, figure_dir, MAIN_ANALYSIS_ID
from .figure_utils import fit_penalized_b_spline, materialize_image, get_mean_derivative_penalized_b_spline, get_bootstrap_ci_mean_derivative_penalized_b_spline
from ..constants import constants



def _size_growth_slope_by_year_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    years = sorted(df['year'].unique().tolist())
    slopes_with_cis = []
    for y in years:
        df_y = df[df['year'] == y].copy()
        mean_value, ci_low, ci_high = get_bootstrap_ci_mean_derivative_penalized_b_spline(df=df_y, xaxis=xaxis, yaxis=yaxis, lam=lam, n_boots=n_boots)
        slopes_with_cis.append({
            'year': y,
            'size_growth_slope': mean_value,
            'ci_low': ci_low,
            'ci_high': ci_high
        })

    slopes_with_cis = pd.DataFrame(slopes_with_cis)
    return slopes_with_cis


def _plot_size_growth_slope_vs_urbanization(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']
    inset_font_size = style_config['inset_font_size']
    inset_tick_font_size = style_config['inset_tick_font_size']

    x_axis = 'urban_population_share'
    y_axis = 'size_growth_slope'

    x_axis_label = 'Urban population share'
    y_axis_label = 'Growth advantage of large cities\n(size-growth slope)'

    x_axis_inset = 'takeoff_year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Takeoff year\n(Year 20% urban)'
    y_axis_inset_label = 'Size-growth slope'

    lam = constants['PENALTY_SLOPE_SPLINE']
    color = px.colors.qualitative.Plotly[0]

    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)

    ax.plot(x, y, color=color, linewidth=2)
    ax.fill_between(x, ci_low, ci_high, color=color, alpha=0.2)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)

    ax_inset = fig.add_axes([0.35, 0.75, 0.1, 0.1])
    x_inset, y_inset, ci_low_inset, ci_high_inset = fit_penalized_b_spline(df=df, xaxis=x_axis_inset, yaxis=y_axis_inset, lam=lam)
    ax_inset.plot(x_inset, y_inset, color=color, linewidth=2)
    ax_inset.fill_between(x_inset, ci_low_inset, ci_high_inset, color=color, alpha=0.2)
    ax_inset.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    ax_inset.set_xlabel(x_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(y_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax.set_title('Global cross-section', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax_inset)
    sns.despine(ax=ax)
    return fig, ax


def _plot_size_growth_curve_kor_by_year(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']
    inset_font_size = style_config['inset_font_size']
    inset_tick_font_size = style_config['inset_tick_font_size']

    years_size_growth_curve = [1975, 2015]
    x_axis = 'log_population'
    y_axis = 'log_growth'

    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = 'Size-growth slope'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    df_size_growth_slope = _size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis=x_axis, yaxis=y_axis, lam=lam, n_boots=n_boots)

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6], px.colors.qualitative.Plotly[7]]

    for i, yr in enumerate(years_size_growth_curve):
        pop_growth_kor_t = df_size_vs_growth[df_size_vs_growth['year'] == yr] 
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_kor_t, xaxis=x_axis, yaxis=y_axis, lam=100)
        color = colors[i]
        ax.plot(x, y, label=f'{yr}-{yr + 10}', color=color)
        ax.fill_between(x, ci_low, ci_high, alpha=0.2, color=color)   


    ax_inset = fig.add_axes([0.63, 0.75, 0.1, 0.1])
    ax_inset.plot(df_size_growth_slope[x_axis_inset], df_size_growth_slope[y_axis_inset], color=px.colors.qualitative.Plotly[0], linewidth=1, marker='o', markersize=2)
    ax_inset.fill_between(df_size_growth_slope[x_axis_inset], df_size_growth_slope['ci_low'], df_size_growth_slope['ci_high'], alpha=0.2, color=px.colors.qualitative.Plotly[0])
    
    ax_inset.set_xlabel(x_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(y_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_yticks([0, 0.1, 0.05])
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.legend(loc='lower right', fontsize=axis_font_size, frameon=False)
    ax.set_title('South Korea', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    sns.despine(ax=ax_inset)
    return fig, ax



def _plot_size_growth_slope_usa_by_year(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    df_slopes_with_cis = _size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam, n_boots=n_boots)

    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']

    x_axis = 'year'
    y_axis = 'size_growth_slope'

    x_axis_label = 'Year'
    y_axis_label = 'Growth advantage of large cities\n(size-growth slope)'
    color = px.colors.qualitative.Plotly[0]

    ax.plot(df_slopes_with_cis[x_axis], df_slopes_with_cis[y_axis], color=color, linewidth=2, marker='o')
    ax.fill_between(df_slopes_with_cis[x_axis], df_slopes_with_cis['ci_low'], df_slopes_with_cis['ci_high'], color=color, alpha=0.2)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('USA', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax


def _plot_size_growth_curve_usa_by_epoch(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    title_font_size = style_config['title_font_size']
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'
    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    colors = [px.colors.qualitative.Plotly[7], px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    epochs = sorted(df_size_vs_growth_normalized['epoch'].unique().tolist())
    for i, e in enumerate(epochs):
        pop_growth_e = df_size_vs_growth_normalized[df_size_vs_growth_normalized['epoch'] == e].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_e, xaxis=x_axis, yaxis=y_axis, lam=lam)

        average_log_growth_e = df_average_growth[df_average_growth['epoch'] == e]['log_average_growth'].mean()

        color = colors[i]
        ax.plot(x, average_log_growth_e + y, color=color, label=e)
        ax.fill_between(x, average_log_growth_e + ci_low, average_log_growth_e + ci_high, color=color, alpha=0.2)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('USA', fontsize=title_font_size, fontfamily=font_family)
    ax.legend(fontsize=axis_font_size, loc='upper left', frameon=False)
    sns.despine(ax=ax)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(), TableNamesResource().names.usa.figures.usa_average_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.world.figures.world_size_growth_slopes_urbanization(), TableNamesResource().names.world.figures.world_size_vs_growth()],
    group_name="figures"
)
def figure_3(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 3")
    figure_file_name = 'figure_3.png'
    figure_path = os.path.join(figure_dir, figure_file_name)

    fig, axes = plt.subplots(2,2, figsize=(10, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.25})
    ax1, ax2, ax3, ax4 = axes.flatten()

    world_size_growth_slopes = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_growth_slopes_urbanization()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_size_growth_slope_vs_urbanization(fig=fig, ax=ax1, style_config=style_config, df=world_size_growth_slopes)

    n_boots = 100
    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_size_vs_growth()}
    WHERE country = 'KOR'
    AND analysis_id = {MAIN_ANALYSIS_ID}
    """
    kor_size_vs_growth = pd.read_sql(q, con=postgres.get_engine())
    _plot_size_growth_curve_kor_by_year(fig=fig, ax=ax2, style_config=style_config, df_size_vs_growth=kor_size_vs_growth, n_boots=n_boots)


    usa_size_vs_growth = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_size_vs_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_size_growth_slope_usa_by_year(fig=fig, ax=ax3, style_config=style_config, df_size_vs_growth=usa_size_vs_growth, n_boots=n_boots)


    usa_size_vs_growth_normalized = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_size_vs_growth_normalized()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    usa_average_growth = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_average_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_size_growth_curve_usa_by_epoch(fig=fig, ax=ax4, style_config=style_config, df_size_vs_growth_normalized=usa_size_vs_growth_normalized, df_average_growth=usa_average_growth)
    
    fig.savefig(figure_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return materialize_image(path=figure_path)