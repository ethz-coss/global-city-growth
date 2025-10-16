# src/orchestration/defs/assets/figures/figure_3.py
from re import A
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

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, style_inset_axes, style_config, region_colors, apply_figure_theme
from .figure_io import read_pandas, save_figure, materialize_image
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_rank_size_slope_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, base_year: int, plot_legend: bool, title: str) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = f'year_since_{base_year}'
    y_axis = 'rank_size_slope_change'
    group_col = 'urban_population_share_group'

    x_axis_label = f'Time since {base_year}'

    alpha_label = r'$(\alpha_t \ / \ \alpha_{1975} - 1)$' if base_year == 1975 else r'$(\alpha_t \ / \ \alpha_{2025} - 1)$'
    y_axis_label = f'Change in concentration since {base_year}\n' + alpha_label
    label_font_size = style_config['label_font_size']

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]
    x_bins = np.arange(5, 55, 10)

    groups = df[group_col].unique().tolist()
    for i, g in enumerate(groups):
        df_g = df[df[group_col] == g]
        sns.regplot(data=df_g, x=x_axis, y=y_axis, ax=ax, ci=95, fit_reg=False, x_bins=x_bins, color=colors[i], line_kws={'linewidth': 1.5})

    if plot_legend:
        ax.scatter([None], [None], marker='o', color=colors[0], label='Late (0-60%)')
        ax.scatter([None], [None], marker='o', color=colors[1], label='Early (60-100%)')
        ax.scatter([None], [None], marker='o', color='grey', label='All (0-100%)')
        ax.legend(loc='upper center', fontsize=label_font_size, frameon=False, title='Urban population share 1975', title_fontsize=label_font_size, ncol=1)
        
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)
    ax.set_ylim(-0.019, 0.21)
    return fig, ax


def _plot_rank_size_slope_change_curves(fig: plt.Figure, ax: plt.Axes, year_base: int, year_middle: int, year_end: int, df: pd.DataFrame, x_axis: str, y_axis: str, colors: List[str], linewidth: int, markersize: int) -> None:
    df_early = df[(df['year_base'] == year_base) & (df['year'] <= year_middle) & (df['year'] >= year_base)].copy()
    df_late = df[(df['year_base'] == year_middle) & (df['year'] >= year_middle) & (df['year'] <= year_end)].copy()
    ax.plot(df_early[x_axis] - year_base, df_early[y_axis], label=f'{year_base}-{year_middle}', color=colors[0], marker='o', linewidth=linewidth, markersize=markersize, linestyle='--')
    ax.plot(df_late[x_axis] - year_middle, df_late[y_axis], label=f'{year_middle}-{year_end}', color=colors[1], marker='o', linewidth=linewidth, markersize=markersize, linestyle='--')
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

    ax_inset = fig.add_axes([0.62, 0.75, 0.1, 0.1])
    _plot_rank_size_slope_change_curves(fig=fig, ax=ax_inset, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_kor, x_axis=x_axis, y_axis=y_axis, colors=colors, linewidth=1, markersize=2)
    ax_inset.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    return fig, ax


def _plot_barchart_concentration_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, y_axis: str, y_axis_label: str) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'year'
    y_axis = y_axis
    group_col = 'urban_population_share_group'

    x_axis_label = 'Year'
    y_axis_label = y_axis_label
    label_font_size = style_config['label_font_size']

    x_bins = [1975, 2025, 2075]
    df_plot = df[df['year'].isin(x_bins)]
    df_plot_world = df_plot.copy()
    df_plot_world[group_col] = 'World'
    df_plot_all = pd.concat([df_plot, df_plot_world], axis=0)

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6], 'grey']
    sns.barplot(data=df_plot_all, x=x_axis, y=y_axis, ax=ax, errorbar=('ci', 95), dodge=True, orient='v', hue=group_col, palette=colors, fill=True, saturation=1, legend=False, err_kws={'linewidth': 1.5, 'alpha': 0.8}, native_scale=True)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.legend(fontsize=label_font_size, frameon=False, ncol=2, loc='lower left')
    ax.set_xticks(x_bins)
    return fig, ax

def _plot_urban_population_share_in_cities_above_one_million_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    y_axis = 'urban_population_share_cities_above_one_million'
    y_axis_label = 'Share of urban population in cities above 1M'
    fig, ax =_plot_barchart_concentration_change_by_urbanization_group(fig=fig, ax=ax, df=df, y_axis=y_axis, y_axis_label=y_axis_label)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    return fig, ax


def _plot_rank_size_slope_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    y_axis = 'rank_size_slope'
    y_axis_label = 'Rank size slope'
    fig, ax = _plot_barchart_concentration_change_by_urbanization_group(fig=fig, ax=ax, df=df, y_axis=y_axis, y_axis_label=y_axis_label)
    ax.set_ylim(0.9, 1.3)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_change_1975_2025(), TableNamesResource().names.world.figures.world_rank_size_slopes_change_2025_2075(), TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_change(),TableNamesResource().names.world.figures.world_population_share_cities_above_1m()],
    group_name="figures"
)
def figure_3(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 3")
    apply_figure_theme()
    figure_file_name = 'figure_3.png'

    fig = plt.figure(figsize=(10, 10))
    grid = plt.GridSpec(2, 4, wspace=0.65, hspace=0.25)
    ax1 = fig.add_subplot(grid[0, 0:2])
    ax2 = fig.add_subplot(grid[0, 2:4])
    ax3 = fig.add_subplot(grid[1, 0:2])
    ax4 = fig.add_subplot(grid[1, 2])
    ax5 = fig.add_subplot(grid[1, 3])

    engine = postgres.get_engine()

    world_rank_size_slopes_change_1975_2025 = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_1975_2025(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax1, df=world_rank_size_slopes_change_1975_2025, base_year=1975, plot_legend=False, title='Data')

    usa_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_size_slope_change_usa_kor(fig=fig, ax=ax2, df_usa=usa_rank_size_slopes_change, df_kor=kor_rank_size_slopes_change)

    world_rank_size_slopes_change_2025_2075 = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_2025_2075(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax3, df=world_rank_size_slopes_change_2025_2075, base_year=2025, plot_legend=True, title='Projections')

    world_population_share_cities_above_1m = read_pandas(engine=engine, table=tables.names.world.figures.world_population_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 2075")
    _plot_urban_population_share_in_cities_above_one_million_by_urbanization_group(fig=fig, ax=ax4, df=world_population_share_cities_above_1m)

    world_rank_size_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 2075")
    _plot_rank_size_slope_by_urbanization_group(fig=fig, ax=ax5, df=world_rank_size_slopes)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4], left_side=[True, True, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)