# src/orchestration/defs/assets/figures/figure_2.py
import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import os
from typing import Tuple, Dict, Any, List

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, plot_spline_with_ci, style_inset_axes, apply_figure_theme, style_config
from .figure_stats import fit_penalized_b_spline, size_growth_slope_by_year_with_cis
from .figure_io import read_pandas, save_figure, materialize_image, MAIN_ANALYSIS_ID
from ..constants import constants

def _plot_size_growth_slope_vs_urbanization(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'urban_population_share'
    y_axis = 'size_growth_slope'

    title = 'Global cross-section'
    x_axis_label = 'Urban population share'
    y_axis_label = 'Growth advantage of large cities\n(size-growth slope)'

    lam = constants['PENALTY_SLOPE_SPLINE']
    color = px.colors.qualitative.Plotly[0]

    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)
    plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)
    sns.despine(ax=ax)
    return fig, ax


def _plot_size_growth_curve_kor_by_year(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    # Main plot
    years_size_growth_curve = [1975, 1995, 2015]

    x_axis = 'log_population'
    y_axis = 'log_growth'

    title = 'South Korea'
    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    colors = [px.colors.qualitative.Plotly[7], px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    for i, yr in enumerate(years_size_growth_curve):
        pop_growth_kor_t = df_size_vs_growth[df_size_vs_growth['year'] == yr] 
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_kor_t, xaxis=x_axis, yaxis=y_axis, lam=lam)
        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color, label=f'{yr}-{yr + 10}')


    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower right')

    # Inset plot
    df_size_growth_slope = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis=x_axis, yaxis=y_axis, lam=lam, n_boots=n_boots)

    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = 'Size-growth slope'
    color_inset = px.colors.qualitative.Plotly[0]

    ax_inset = fig.add_axes([0.63, 0.75, 0.1, 0.1])
    ax_inset.plot(df_size_growth_slope[x_axis_inset], df_size_growth_slope[y_axis_inset], color=color_inset, linewidth=1, marker='o', markersize=2)
    ax_inset.fill_between(df_size_growth_slope[x_axis_inset], df_size_growth_slope['ci_low'], df_size_growth_slope['ci_high'], color=color_inset, alpha=0.2)
    ax_inset.set_yticks([0, 0.1, 0.05])
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax


def _plot_size_growth_curve_usa_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    
    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = 'USA'
    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    colors = [px.colors.qualitative.Plotly[7], px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    epochs = sorted(df_size_vs_growth_normalized['epoch'].unique().tolist())
    for i, e in enumerate(epochs):
        pop_growth_e = df_size_vs_growth_normalized[df_size_vs_growth_normalized['epoch'] == e].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_e, xaxis=x_axis, yaxis=y_axis, lam=lam)

        average_log_growth_e = df_average_growth[df_average_growth['epoch'] == e]['log_average_growth'].mean()

        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_e + y, ci_low=average_log_growth_e + ci_low, ci_high=average_log_growth_e + ci_high, color=color, label=e)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='upper right')

    # Inset plot
    df_slopes_with_cis = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam, n_boots=n_boots)

    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = 'Size-growth slope'
    color_inset = px.colors.qualitative.Plotly[0]
    
    ax_inset = fig.add_axes([0.63, 0.33, 0.1, 0.1])
    ax_inset.plot(df_slopes_with_cis[x_axis_inset], df_slopes_with_cis[y_axis_inset], color=color_inset, linewidth=1, marker='o', markersize=2)
    ax_inset.fill_between(df_slopes_with_cis[x_axis_inset], df_slopes_with_cis['ci_low'], df_slopes_with_cis['ci_high'], color=color_inset, alpha=0.2)
    ax_inset.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
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

@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(), TableNamesResource().names.usa.figures.usa_average_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(), TableNamesResource().names.world.figures.world_size_vs_growth(), TableNamesResource().names.world.figures.world_region_regression_with_urbanization_controls()],
    group_name="figures"
)
def figure_2(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 2")
    apply_figure_theme()
    figure_file_name = 'figure_2.png'

    fig, axes = plt.subplots(2,2, figsize=(10, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.25})
    ax1, ax2, ax3, ax4 = axes.flatten()
    engine = postgres.get_engine()

    world_size_growth_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_slope_vs_urbanization(fig=fig, ax=ax1, df=world_size_growth_slopes)

    n_boots = 1000
    kor_size_vs_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_size_growth_curve_kor_by_year(fig=fig, ax=ax2, df_size_vs_growth=kor_size_vs_growth, n_boots=n_boots)

    usa_size_vs_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID)
    usa_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    usa_average_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_curve_usa_by_epoch(fig=fig, ax=ax4, df_size_vs_growth=usa_size_vs_growth, df_size_vs_growth_normalized=usa_size_vs_growth_normalized, df_average_growth=usa_average_growth, n_boots=n_boots)

    world_region_regression_with_urbanization_controls = read_pandas(engine=engine, table=tables.names.world.figures.world_region_regression_with_urbanization_controls(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_region_regression_with_urbanization_controls(fig=fig, ax=ax3, df=world_region_regression_with_urbanization_controls)
    
    annotate_letter_label(axes=[ax1, ax2, ax4, ax3], left_side=[False, False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)