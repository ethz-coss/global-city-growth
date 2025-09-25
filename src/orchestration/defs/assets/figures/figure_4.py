# src/orchestration/defs/assets/figures/figure_4.py
import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import plotly.express as px
import seaborn as sns
import os
import numpy as np
from typing import Tuple, Dict, List
from matplotlib import ticker as mtick
from typing import Callable

from sqlalchemy.sql import true


from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, plot_spline_with_ci, style_inset_axes, style_config, region_colors, apply_figure_theme
from .figure_stats import fit_penalized_b_spline, size_growth_slope_by_year_with_cis, clustered_boostrap_ci
from .figure_io import read_pandas, save_figure, materialize_image, MAIN_ANALYSIS_ID
from ..constants import constants



def _plot_rank_size_slope_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'year_since_1975'
    y_axis = 'rank_size_slope_change'

    x_axis_label = 'Time since 1975'
    y_axis_label = 'Change in concentration since 1975\n' + r'$(\alpha_{t} \ / \ \alpha_{1975} - 1)$'
    label_font_size = style_config['label_font_size']

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    lam = constants['PENALTY_SLOPE_SPLINE']

    groups = sorted(df['urban_population_share_group'].unique())
    
    for i, g in enumerate(groups):
        df_g = df[df['urban_population_share_group'] == g]
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_g, xaxis=x_axis, yaxis=y_axis, lam=lam)
        plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=colors[i], label=f'{g}%')

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.set_ylim(-0.02, 0.22)
    ax.legend(loc='upper center', fontsize=label_font_size, frameon=False, title='Urban population share 1975', title_fontsize=label_font_size, ncol=2)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax


def _plot_rank_size_slope_change_curves(fig: plt.Figure, ax: plt.Axes, year_base: int, year_middle: int, year_end: int, df: pd.DataFrame, x_axis: str, y_axis: str, colors: List[str], linewidth: int, markersize: int) -> None:
    df_early = df[(df['year_base'] == year_base) & (df['year'] <= year_middle) & (df['year'] >= year_base)].copy()
    df_late = df[(df['year_base'] == year_middle) & (df['year'] >= year_middle) & (df['year'] <= year_end)].copy()
    ax.plot(df_early[x_axis] - year_base, df_early[y_axis], label=f'{year_base}-{year_middle}', color=colors[0], marker='o', linewidth=linewidth, markersize=markersize)
    ax.plot(df_late[x_axis] - year_middle, df_late[y_axis], label=f'{year_middle}-{year_end}', color=colors[1], marker='o', linewidth=linewidth, markersize=markersize)
    return fig, ax

def _plot_rank_size_slope_change_usa_kor(fig: plt.Figure, ax: plt.Axes, df_usa: pd.DataFrame, df_kor: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    # Main plot (USA)
    x_axis = 'year'
    y_axis = 'rank_size_slope_change'

    title = 'USA'
    x_axis_label = r'Time since $t_0$'
    y_axis_label = r'Change in concentration since $t_0$' + '\n' + r'($\alpha_{t} \ / \ \alpha_{t_0} - 1)$'
    year_base, year_middle, year_end = 1850, 1930, 2020

    _plot_rank_size_slope_change_curves(fig=fig, ax=ax, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_usa, x_axis=x_axis, y_axis=y_axis, colors=colors, linewidth=2, markersize=5)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower right')

    # Inset plot (Korea)
    inset_title = 'South Korea'
    x_axis_inset_label = r'Time since $t_0$'
    y_axis_inset_label = r'$\alpha_{t} \ / \ \alpha_{t_0} - 1$'
    year_base, year_middle, year_end = 1975, 2000, 2025

    ax_inset = fig.add_axes([0.64, 0.74, 0.1, 0.1])
    _plot_rank_size_slope_change_curves(fig=fig, ax=ax_inset, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_kor, x_axis=x_axis, y_axis=y_axis, colors=colors, linewidth=1, markersize=2)
    ax_inset.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    return fig, ax


def _plot_rank_size_slope_decade_change_by_region(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    region_col = 'region'

    x_axis = 'year'
    y_axis = 'rank_size_slope_decade_change'

    x_axis_label = 'Year'
    y_axis_label = 'Change in concentration over one decade\n' + r'$(\alpha_{t + 10} \ / \ \alpha_{t} - 1)$'
    label_font_size = style_config['label_font_size']

    lam = constants['PENALTY_SLOPE_SPLINE']

    regions = sorted(df[region_col].unique())
    for r in regions:
        df_r = df[df[region_col] == r]
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_r, xaxis=x_axis, yaxis=y_axis, lam=lam)
        color = region_colors[r]
        plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color, label=r, alpha_fill=0.1)

    ax.plot([2020, 2020], [-0.005, 0.05], color='grey', linewidth=1, linestyle='--')
    ax.annotate('Forecast', [2035, 0.05], fontsize=label_font_size, ha='center')
    ax.annotate('Data', [1996, 0.05], fontsize=label_font_size, ha='center')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.legend(loc='upper right', fontsize=label_font_size, frameon=False, ncol=2)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax


def _get_mean_bootstrap_ci_region_year(df: pd.DataFrame, estimator: Callable[[pd.DataFrame], float], nboots: int) -> pd.DataFrame:
    rows = []
    for (region, year), g in df.groupby(['region', 'year'], sort=True):
        median, ci_low, ci_high = clustered_boostrap_ci(
            estimator=estimator, df=g, cluster_col='country', nboots=nboots
        )
        rows.append({
            'region': region, 'year': year,
            'median': median, 'ci_low': ci_low, 'ci_high': ci_high
        })
    return pd.DataFrame(rows)


def _plot_bars_with_cis(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, y_axis_label: str, x_axis_label: str) -> Tuple[plt.Figure, plt.Axes]:
    regions = sorted(df['region'].unique())
    years = sorted(df['year'].unique())
    ys = np.arange(len(years))
    shifts = np.linspace(-0.2, 0.2, len(regions))
    for i, r in enumerate(regions):
        for j, y in enumerate(years):
            median_rs, ci_low_rs, ci_high_rs = df[(df['region'] == r) & (df['year'] == y)][['median', 'ci_low', 'ci_high']].values[0]
            yerr_rs =[
                [ci_high_rs - median_rs],
                [median_rs - ci_low_rs]
            ]
            ax.scatter([median_rs], [ys[j] + shifts[i]], color=region_colors[r], marker='o', linestyle='None', s=20)
            ax.errorbar([median_rs], [ys[j] + shifts[i]], xerr=yerr_rs, color=region_colors[r], linestyle='None', linewidth=1)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.set_yticks(ys)
    ax.set_yticklabels(years)
    return fig, ax

def _plot_rank_size_slopes_bars(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, nboots: int) -> Tuple[plt.Figure, plt.Axes]:
    bars_with_cis = _get_mean_bootstrap_ci_region_year(df=df, estimator=lambda x: x['rank_size_slope'].mean(), nboots=nboots)
    y_axis_label = ''
    x_axis_label = r'Concentration ($\alpha$)'
    _plot_bars_with_cis(fig=fig, ax=ax, df=bars_with_cis, y_axis_label=y_axis_label, x_axis_label=x_axis_label)
    return fig, ax

def _plot_population_shares_in_cities_above_one_million_bars(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, nboots: int) -> Tuple[plt.Figure, plt.Axes]:
    bars_with_cis = _get_mean_bootstrap_ci_region_year(df=df, estimator=lambda x: np.sum(x['urban_population'] * x['population_share_cities_above_one_million']) / np.sum(x['urban_population']), nboots=nboots)
    y_axis_label = ''
    x_axis_label = 'Urban population share in\ncities above 1M'
    fig, ax = _plot_bars_with_cis(fig=fig, ax=ax, df=bars_with_cis, y_axis_label=y_axis_label, x_axis_label=x_axis_label)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    return fig, ax



@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_change_by_urbanization_group(), TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_decade_change(), TableNamesResource().names.world.figures.world_population_share_cities_above_1m(), TableNamesResource().names.world.figures.world_rank_size_slopes()],
    group_name="figures"
)
def figure_4(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 4")
    apply_figure_theme()
    figure_file_name = 'figure_4.png'

    fig = plt.figure(figsize=(10, 10))
    grid_main = plt.GridSpec(2, 1, hspace=0.2)
    grid_top = grid_main[0].subgridspec(1, 2, wspace=0.3)
    ax1 = fig.add_subplot(grid_top[0, 0])
    ax2 = fig.add_subplot(grid_top[0, 1])

    grid_bottom = grid_main[1].subgridspec(2, 2, width_ratios=[2, 1], hspace=0.4, wspace=0.15)
    ax3 = fig.add_subplot(grid_bottom[:, 0]) 
    ax4 = fig.add_subplot(grid_bottom[0, 1]) 
    ax5 = fig.add_subplot(grid_bottom[1, 1]) 

    engine = postgres.get_engine()

    world_rank_size_slopes_change_by_urbanization_group = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_by_urbanization_group(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 2025")
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax1, df=world_rank_size_slopes_change_by_urbanization_group)

    usa_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_size_slope_change_usa_kor(fig=fig, ax=ax2, df_usa=usa_rank_size_slopes_change, df_kor=kor_rank_size_slopes_change)


    world_rank_size_slopes_decade_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_decade_change(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_decade_change_by_region(fig=fig, ax=ax3, df=world_rank_size_slopes_decade_change)

    nboots = 1000
    world_rank_size_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes(), analysis_id=MAIN_ANALYSIS_ID, where="year IN (1980, 2020, 2060)")
    _plot_rank_size_slopes_bars(fig=fig, ax=ax4, df=world_rank_size_slopes, nboots=nboots)

    world_population_share_cities_above_1m = read_pandas(engine=engine, table=tables.names.world.figures.world_population_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year IN (1980, 2020, 2060)")
    _plot_population_shares_in_cities_above_one_million_bars(fig=fig, ax=ax5, df=world_population_share_cities_above_1m, nboots=nboots)


    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5], left_side=[True, True, True, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)