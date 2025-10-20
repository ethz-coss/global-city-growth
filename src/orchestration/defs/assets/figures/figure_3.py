# src/orchestration/defs/assets/figures/figure_3.py
import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import numpy as np
from typing import Tuple
from matplotlib import ticker as mtick
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, style_config, apply_figure_theme
from .figure_io import read_pandas, save_figure, materialize_image
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_rank_size_slope_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'year'
    y_axis = 'rank_size_slope'
    group_col = 'urban_population_share_group'

    x_axis_label = 'Year'
    y_axis_label = 'Rank-size slope ' + r'$\alpha$'

    x_bins = [1975, 2025, 2100]
    df_plot = df[df['year'].isin(x_bins)]
    df_plot_world = df_plot.copy()
    df_plot_world[group_col] = 'World'
    df_plot_all = pd.concat([df_plot, df_plot_world], axis=0)

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6], 'grey']
    sns.barplot(data=df_plot_all, x=x_axis, y=y_axis, ax=ax, errorbar=('ci', 95), dodge=True, orient='v', hue=group_col, palette=colors, fill=True, saturation=1, legend=False, err_kws={'linewidth': 1.5, 'alpha': 0.8})

    group_labels = ['Late urbanizers (0-60%)', 'Early urbanizers (60-100%)', 'World (0-100%)']
    handles, labels = ax.get_legend_handles_labels()
    for i, group_label in enumerate(group_labels):
        proxy = Patch(facecolor=colors[i], label=group_label)
        handles.append(proxy)
        labels.append(group_label)
    
    ax.legend(handles=handles, labels=labels, frameon=False, title='Urban population share in 1975', loc='upper left')

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.set_ylim(0.9, 1.4)
    return fig, ax

def _plot_region_regression_with_urbanization_controls(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    df = df.set_index('region')

    bar_width = 0.35
    index = np.arange(len(df.index))
    label_font_size = style_config['label_font_size']
    inset_label_font_size = style_config['inset_label_font_size']

    no_control_error = [df['coeff_no_control'] - df['ci_low_no_control'], df['ci_high_no_control'] - df['coeff_no_control']]
    with_control_error = [df['coeff_with_control'] - df['ci_low_with_control'], df['ci_high_with_control'] - df['coeff_with_control']]

    ax.bar(index - bar_width/2, df['coeff_no_control'], bar_width,
                yerr=no_control_error, capsize=5,
                label='Without control', color='skyblue', ecolor='gray')

    ax.bar(index + bar_width/2, df['coeff_with_control'], bar_width,
                yerr=with_control_error, capsize=5,
                label='With control', color='lightcoral', ecolor='gray')

    ax.axhline(0, color='grey', linewidth=0.8, linestyle='--')
    ax.annotate('Global average', 
                xy=[2.5, 0], 
                xytext=[2, 0.005], 
                arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
                fontsize=label_font_size)

    ax.set_xticks(index)
    ax.set_xticklabels(df.index, rotation=0, ha="center")
    style_axes(ax=ax, xlabel='', ylabel='Deviation of region size-growth slope\nfrom global average')
    ax.legend(fontsize=inset_label_font_size, loc='upper center', frameon=False, title='Urban population share control') # here we want a smaller legend font so we style it ourselves
    return fig, ax



def _plot_bar_share_cities_above_1m(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, year: int, x_axis_label: str, hatch: str) -> Tuple[plt.Figure, plt.Axes]:
    df_y = df[df['year'] == year].copy()
    df_y['x_axis_label'] = x_axis_label

    n_before = len(ax.patches)
    sns.barplot(data=df_y, x='x_axis_label', y='total_population_share_cities_above_one_million', ax=ax, color='grey', weights='population', native_scale=False, saturation=1)
    new_bars = ax.patches[n_before:]
    for p in new_bars:
        p.set_hatch(hatch)
    return fig, ax

from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D

def _plot_world_population_share_cities_above_1m_scenarios(fig: plt.Figure, ax: plt.Axes, extr: pd.DataFrame, prop_growth: pd.DataFrame, inc_returns: pd.DataFrame, model: pd.DataFrame, data: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=data, year=1975, x_axis_label='1975', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=extr, year=2025, x_axis_label='2025', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=prop_growth, year=2100, x_axis_label='2100\n(PG)',  hatch='-')
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=model, year=2100, x_axis_label='2100\n(OM)', hatch='O')
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=extr, year=2100, x_axis_label='2100\n(EX)', hatch='x')
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=inc_returns, year=2100, x_axis_label='2100\n(IR)', hatch='/')

    legend_labels = ['PG = Proportional growth (--)', 'OM = Our model (O)', 'EX = Extrapolation (X)', 'IR = Increasing returns (/)']
    handles, labels = ax.get_legend_handles_labels()
    for legend_label in legend_labels:
        proxy = Line2D([], [], linestyle='none', marker=None)
        handles.append(proxy)
        labels.append(legend_label)
    ax.legend(handles=handles, labels=labels, frameon=False, title='', loc='upper left', handlelength=0, handletextpad=0)

    style_axes(ax=ax, xlabel=' ', ylabel='Share of world population in 1M+ cities')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_region_regression_with_urbanization_controls(), TableNamesResource().names.world.figures.world_rank_size_slopes(), TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_change(),TableNamesResource().names.world.figures.world_urb_pop_share_cities_above_1m(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_extr(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_prop_growth(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_inc_returns(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m()],
    group_name="figures"
)
def figure_3(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 3")
    apply_figure_theme()
    figure_file_name = 'figure_3.png'

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    ax1, ax2, ax3 = axes

    engine = postgres.get_engine()

    world_region_regression_with_urbanization_controls = read_pandas(engine=engine, table=tables.names.world.figures.world_region_regression_with_urbanization_controls(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_region_regression_with_urbanization_controls(fig=fig, ax=ax1, df=world_region_regression_with_urbanization_controls)

    world_rank_size_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_by_urbanization_group(fig=fig, ax=ax2, df=world_rank_size_slopes)

    world_share_cities_above_1m_projections_extr = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_extr(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_prop_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_prop_growth(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_inc_returns = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_inc_returns(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_model = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year > 2025")
    world_share_cities_above_1m_data = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 1975")

    _plot_world_population_share_cities_above_1m_scenarios(fig=fig, ax=ax3, extr=world_share_cities_above_1m_projections_extr, prop_growth=world_share_cities_above_1m_projections_prop_growth, inc_returns=world_share_cities_above_1m_projections_inc_returns, model=world_share_cities_above_1m_projections_model, data=world_share_cities_above_1m_data)

    annotate_letter_label(axes=[ax1, ax2, ax3], left_side=[False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)