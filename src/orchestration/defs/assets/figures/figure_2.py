# src/orchestration/defs/assets/figures/figure_2.py
import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
from typing import Tuple
from matplotlib import ticker as mtick
from matplotlib.lines import Line2D

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, plot_spline_with_ci, style_inset_axes, apply_figure_theme, style_config
from ..stats_utils import fit_penalized_b_spline, size_growth_slope_by_year_with_cis
from .figure_io import read_pandas, save_figure, materialize_image
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_size_growth_slope_vs_urbanization(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'urban_population_share'
    y_axis = 'size_growth_slope'

    x_axis_label = 'Urban population share'
    y_axis_label = 'Growth advantage of large cities\n' + r'(Size-growth slope $\beta_t$)'

    lam = constants['PENALTY_SLOPE_SPLINE']
    color = px.colors.qualitative.Plotly[0]

    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)
    plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    sns.despine(ax=ax)
    return fig, ax

def _plot_size_growth_curve_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = r'$\leftarrow$' + ' Global cross-section' + r'$\rightarrow$'
    x_axis_label = r'Size ($\log_{10}$ population)'
    y_axis_label = r'Growth rate ($\log_{10}$ growth)'
    label_font_size = style_config['label_font_size']

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    groups = sorted(df_size_vs_growth_normalized['urban_population_share_group'].unique().tolist())
    for i, g in enumerate(groups):
        pop_growth_g = df_size_vs_growth_normalized[df_size_vs_growth_normalized['urban_population_share_group'] == g].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_g, xaxis=x_axis, yaxis=y_axis, lam=lam)
        average_log_growth_g = df_average_growth[df_average_growth['urban_population_share_group'] == g]['log_average_growth'].mean()
        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_g + y, ci_low=average_log_growth_g + ci_low, ci_high=average_log_growth_g + ci_high, color=color, label=None)


    ax.plot([None], [None], marker='o', linewidth=1, color=colors[0], label='Late urbanizers (0-60%)')
    ax.plot([None], [None], marker='o', linewidth=1, color=colors[1], label='Early urbanizers (60-100%)')
    ax.legend(loc='upper center', fontsize=label_font_size, frameon=False, title='Urban population share 1975', title_fontsize=label_font_size, ncol=1)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)
    ax.set_ylim(0.01, 0.17)
    return fig, ax

def _plot_rank_size_slope_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = f'year_since_1975'
    y_axis = 'rank_size_slope_change'
    group_col = 'urban_population_share_group'

    x_axis_label = f'Time since 1975'

    y_axis_label = f'Change in top-heaviness since 1975\n' + r'$(\alpha_t \ / \ \alpha_{1975} - 1)$'

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]
    x_bins = np.arange(5, 55, 10)

    groups = df[group_col].unique().tolist()
    for i, g in enumerate(groups):
        df_g = df[df[group_col] == g]
        sns.regplot(data=df_g, x=x_axis, y=y_axis, ax=ax, ci=95, fit_reg=False, x_bins=x_bins, color=colors[i], line_kws={'linewidth': 1.5})
        
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax

def _plot_size_growth_curve_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, x_axis: str, y_axis: str, lam: float) -> Tuple[plt.Figure, plt.Axes]:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    epochs = sorted(df_size_vs_growth_normalized['epoch'].dropna().unique().tolist())
    for i, e in enumerate(epochs):
        pop_growth_e = df_size_vs_growth_normalized[df_size_vs_growth_normalized['epoch'] == e].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_e, xaxis=x_axis, yaxis=y_axis, lam=lam)

        average_log_growth_e = df_average_growth[df_average_growth['epoch'] == e]['log_average_growth'].mean()

        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_e + y, ci_low=average_log_growth_e + ci_low, ci_high=average_log_growth_e + ci_high, color=color, label=e)

    return fig, ax

def _plot_size_growth_slope_by_year_inset(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, x_axis: str, y_axis: str, lam: float, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    df_slopes_with_cis = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam, n_boots=n_boots)
    color_inset = px.colors.qualitative.Plotly[0]
    ax.plot(df_slopes_with_cis[x_axis], df_slopes_with_cis[y_axis], color=color_inset, linewidth=1, marker='o', markersize=2)
    ax.fill_between(df_slopes_with_cis[x_axis], df_slopes_with_cis['ci_low'], df_slopes_with_cis['ci_high'], color=color_inset, alpha=0.2)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    return fig, ax



def _plot_size_growth_curve_kor_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    def map_year_to_epoch(year: int) -> str:
        if year >= 1975 and year < 1995:
            return '1975-2000'
        elif year >= 2000 and year < 2025:
            return '2000-2025'

    df_size_vs_growth_normalized['epoch'] = df_size_vs_growth_normalized['year'].apply(map_year_to_epoch)
    df_average_growth['epoch'] = df_average_growth['year'].apply(map_year_to_epoch)

    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = 'South Korea'
    x_axis_label = r'Size ($\log_{10}$ population)'
    y_axis_label = r'Growth rate ($\log_{10}$ growth)'

    _plot_size_growth_curve_by_epoch(fig=fig, ax=ax, df_size_vs_growth_normalized=df_size_vs_growth_normalized, df_average_growth=df_average_growth, x_axis=x_axis, y_axis=y_axis, lam=lam)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower right')

    # Inset plot
    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = r'$\beta_t$'

    ax_inset = fig.add_axes([0.1705, 0.36, 0.06, 0.09])

    _plot_size_growth_slope_by_year_inset(fig=fig, ax=ax_inset, df_size_vs_growth=df_size_vs_growth, x_axis=x_axis_inset, y_axis=y_axis_inset, lam=lam, n_boots=n_boots)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax


def _plot_size_growth_curve_usa_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    def map_year_to_epoch(year: int) -> str:
        if year >= 1850 and year < 1930:
            return '1850-1930'
        elif year >= 1930 and year <= 2020:
            return '1930-2020'

    df_size_vs_growth_normalized['epoch'] = df_size_vs_growth_normalized['year'].apply(map_year_to_epoch)
    df_average_growth['epoch'] = df_average_growth['year'].apply(map_year_to_epoch)
        
    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = 'USA'
    x_axis_label = r'Size ($\log_{10}$ population)'
    y_axis_label = r'Growth rate ($\log_{10}$ growth)'

    _plot_size_growth_curve_by_epoch(fig=fig, ax=ax, df_size_vs_growth_normalized=df_size_vs_growth_normalized, df_average_growth=df_average_growth, x_axis=x_axis, y_axis=y_axis, lam=lam)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower center')

    # Inset plot
    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = r'$\beta_t$'
    ax_inset = fig.add_axes([0.445, 0.36, 0.06, 0.09])
    _plot_size_growth_slope_by_year_inset(fig=fig, ax=ax_inset, df_size_vs_growth=df_size_vs_growth, x_axis=x_axis_inset, y_axis=y_axis_inset, lam=lam, n_boots=n_boots)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax


def _plot_rank_size_slope_change_curves(fig: plt.Figure, ax: plt.Axes, year_base: int, year_middle: int, year_end: int, df: pd.DataFrame, x_axis: str, y_axis: str, linewidth: int, markersize: int) -> None:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]
    df_early = df[(df['year_base'] == year_base) & (df['year'] <= year_middle) & (df['year'] >= year_base)].copy()
    df_late = df[(df['year_base'] == year_middle) & (df['year'] >= year_middle) & (df['year'] <= year_end)].copy()
    ax.plot(df_early[x_axis] - year_base, df_early[y_axis], label=f'{year_base}-{year_middle}', color=colors[0], marker='o', linewidth=linewidth, markersize=markersize)
    ax.plot(df_late[x_axis] - year_middle, df_late[y_axis], label=f'{year_middle}-{year_end}', color=colors[1], marker='o', linewidth=linewidth, markersize=markersize)
    return fig, ax

def _plot_rank_size_slope_change_usa_kor(fig: plt.Figure, ax: plt.Axes, df_usa: pd.DataFrame, df_kor: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    # Main plot (USA)
    x_axis = 'year'
    y_axis = 'rank_size_slope_change'

    title = 'USA'
    x_axis_label = r'Time since $t_0$'
    y_axis_label = r'Change in top_heaviness since $t_0$' + '\n' + r'($\alpha_{t} \ / \ \alpha_{t_0} - 1)$'
    year_base, year_middle, year_end = 1850, 1930, 2020

    _plot_rank_size_slope_change_curves(fig=fig, ax=ax, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_usa, x_axis=x_axis, y_axis=y_axis, linewidth=2, markersize=5)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)

    # Inset plot (Korea)
    inset_title = 'South Korea'
    x_axis_inset_label = r'Time since $t_0$'
    y_axis_inset_label = r'$\alpha_{t} \ / \ \alpha_{t_0} - 1$'
    year_base, year_middle, year_end = 1975, 2000, 2025

    ax_inset = fig.add_axes([0.725, 0.34, 0.06, 0.09])
    _plot_rank_size_slope_change_curves(fig=fig, ax=ax_inset, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_kor, x_axis=x_axis, y_axis=y_axis, linewidth=1, markersize=2)
    ax_inset.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    return fig, ax

@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(), TableNamesResource().names.usa.figures.usa_average_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(), TableNamesResource().names.world.figures.world_average_growth(), TableNamesResource().names.world.figures.world_rank_size_slopes_change_1975_2025(), TableNamesResource().names.world.figures.world_size_vs_growth(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(),
    TableNamesResource().names.world.figures.world_rank_size_slopes_change()],
    group_name="figures"
)
def figure_2(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 2")
    apply_figure_theme()
    figure_file_name = 'figure_2.png'

    fig, axes = plt.subplots(2,3, figsize=(15, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.3})
    ax1, ax2, ax3, ax4, ax5, ax6 = axes.flatten()
    engine = postgres.get_engine()

    world_size_growth_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_slope_vs_urbanization(fig=fig, ax=ax1, df=world_size_growth_slopes)

    world_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    world_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_curve_by_urbanization_group(fig=fig, ax=ax2, df_size_vs_growth_normalized=world_size_vs_growth_normalized, df_average_growth=world_average_growth)

    world_rank_size_slopes_change_1975_2025 = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_1975_2025(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax3, df=world_rank_size_slopes_change_1975_2025)

    y_fig = 0.485
    line = Line2D([0.2, 0.8], [y_fig, y_fig],transform=fig.transFigure, color='0.6', lw=1, ls='--', zorder=1000, clip_on=False)
    fig.add_artist(line)

    n_boots = 1000
    kor_size_vs_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    kor_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    kor_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_size_growth_curve_kor_by_epoch(fig=fig, ax=ax4, df_size_vs_growth=kor_size_vs_growth, df_size_vs_growth_normalized=kor_size_vs_growth_normalized, df_average_growth=kor_average_growth, n_boots=n_boots)

    usa_size_vs_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID)
    usa_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    usa_average_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_curve_usa_by_epoch(fig=fig, ax=ax5, df_size_vs_growth=usa_size_vs_growth, df_size_vs_growth_normalized=usa_size_vs_growth_normalized, df_average_growth=usa_average_growth, n_boots=n_boots)


    usa_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_size_slope_change_usa_kor(fig=fig, ax=ax6, df_usa=usa_rank_size_slopes_change, df_kor=kor_rank_size_slopes_change)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5, ax6], left_side=[False, False, True, False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)