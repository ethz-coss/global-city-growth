# src/orchestration/defs/assets/figures/figure_4.py
import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import plotly.express as px
import seaborn as sns
import os
from typing import Tuple, Dict, List
from matplotlib import ticker as mtick


from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, plot_spline_with_ci, style_inset_axes, style_config, region_colors, apply_figure_theme
from .figure_stats import fit_penalized_b_spline, size_growth_slope_by_year_with_cis
from .figure_io import read_pandas, save_figure, materialize_image, MAIN_ANALYSIS_ID
from ..constants import constants



def _plot_rank_size_slope_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'year'
    y_axis = 'rank_size_slope_change'

    x_axis_label = 'Year'
    y_axis_label = 'Change in dominance of large cities\n' + r'$(\alpha_{t} \ / \ \alpha_{1975} - 1)$'
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
    ax.legend(loc='upper center', fontsize=label_font_size, frameon=False, title='Urban population share', title_fontsize=label_font_size, ncol=2)
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
    x_axis_label = 'Time'
    y_axis_label = 'Change in dominance of large cities\n' + r'($\alpha_{t} \ / \ \alpha_{t_0} - 1)$'
    year_base, year_middle, year_end = 1850, 1930, 2020

    _plot_rank_size_slope_change_curves(fig=fig, ax=ax, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_usa, x_axis=x_axis, y_axis=y_axis, colors=colors, linewidth=2, markersize=5)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower right')

    # Inset plot (Korea)
    inset_title = 'Korea'
    x_axis_inset_label = 'Time'
    y_axis_inset_label = r'$\alpha_{t} \ / \ \alpha_{t_0} - 1$'
    year_base, year_middle, year_end = 1975, 2000, 2025

    ax_inset = fig.add_axes([0.2, 0.75, 0.1, 0.1])
    _plot_rank_size_slope_change_curves(fig=fig, ax=ax_inset, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_kor, x_axis=x_axis, y_axis=y_axis, colors=colors, linewidth=1, markersize=2)
    ax_inset.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    return fig, ax


def _plot_rank_size_slope_change_by_region(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    region_col = 'region'

    x_axis = 'year'
    y_axis = 'rank_size_slope_change'

    x_axis_label = 'Year'
    y_axis_label = 'Change in dominance of large cities\n' + r'$(\alpha_{t} \ / \ \alpha_{t_0} - 1)$'
    label_font_size = style_config['label_font_size']

    lam = constants['PENALTY_SLOPE_SPLINE']

    regions = sorted(df[region_col].unique())
    for r in regions:
        df_r = df[df[region_col] == r]
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_r, xaxis=x_axis, yaxis=y_axis, lam=lam)
        color = region_colors[r]
        plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color, label=r, alpha_fill=0.1)

    ax.plot([2025, 2025], [0, 0.3], color='grey', linewidth=1, linestyle='--')
    ax.annotate('Forecast', [2042, 0.28], fontsize=label_font_size, ha='center')
    ax.annotate('Data', [1998, 0.28], fontsize=label_font_size, ha='center')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    ax.legend(loc='lower right', fontsize=label_font_size, frameon=False, ncol=2)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_change_by_urbanization_group(), TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_change()],
    group_name="figures"
)
def figure_4(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 4")
    apply_figure_theme()
    figure_file_name = 'figure_4.png'

    fig, axes = plt.subplots(2, 2, figsize=(10, 10), gridspec_kw={'wspace': 0.35, 'hspace': 0.25})
    ax1, ax2, ax3, ax4 = axes.flatten()
    engine = postgres.get_engine()

    usa_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_size_slope_change_usa_kor(fig=fig, ax=ax1, df_usa=usa_rank_size_slopes_change, df_kor=kor_rank_size_slopes_change)


    world_rank_size_slopes_change_by_urbanization_group = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_by_urbanization_group(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 2025")
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax2, df=world_rank_size_slopes_change_by_urbanization_group)


    world_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="year_base = 1975")
    _plot_rank_size_slope_change_by_region(fig=fig, ax=ax3, df=world_rank_size_slopes_change)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4], left_side=[True, True, True, True])
    
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)