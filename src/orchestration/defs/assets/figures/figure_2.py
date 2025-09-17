# src/orchestration/defs/assets/figures/figure_2.py
import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import geopandas as gpd
import os
from typing import Tuple, Dict, Any

import matplotlib.colors as mcolors

from matplotlib.colors import to_rgba
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.gridspec as gridspec
from sqlalchemy import true

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import create_bicolor_cmap, style_inset_axes, style_axes, annotate_letter_label, region_colors, style_config, apply_figure_theme, plot_spline_with_ci
from .figure_stats import clustered_boostrap_ci, fit_penalized_b_spline
from .figure_io import read_pandas, read_postgis, save_figure, materialize_image, MAIN_ANALYSIS_ID
from ..constants import constants


def _plot_world_map_with_colorbar(fig: plt.Figure, ax: plt.Axes, gdf: gpd.GeoDataFrame) -> Tuple[plt.Figure, plt.Axes]:

    # Map 
    col = 'size_growth_slope'
    midpoint = 0
    
    missing_hatch = '///'
    missing_color = 'white'
    projection_epsg = 4087

    colorbar_label_fontsize = 12
    colorbar_pos = [0.4, 0.13, 0.2, 0.025]
    colorbar_label_pos = 'top'
    colorbar_label = 'Size-growth slope'
    
    gdf_proj = gdf.to_crs(epsg=projection_epsg)
    vmin, vmax = np.nanmin(gdf_proj[col]), np.nanmax(gdf_proj[col])
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    midpoint_frac = (midpoint - vmin) / (vmax - vmin)
    rescaled_cmap = create_bicolor_cmap(cmap_neg='Greys_r', cmap_pos='Blues', midpoint_frac=midpoint_frac)


    ax.set_axis_off()
    divider = make_axes_locatable(ax)
    cax = fig.add_axes(colorbar_pos)

    gdf_proj.plot(column=col, ax=ax, legend=True, cax=cax, cmap=rescaled_cmap, norm=norm, linewidth=0.5, edgecolor='0.5', missing_kwds={
                        "color": missing_color,
                        "edgecolor": to_rgba('#D3D3D3', 0.8),
                        "hatch": missing_hatch,
                    }, legend_kwds={
                        'orientation': "horizontal",
                    })


    for spine in cax.spines.values(): spine.set_visible(False)
    cax.set_xlabel(colorbar_label, fontsize=colorbar_label_fontsize)
    cax.xaxis.set_label_position(colorbar_label_pos)


    # Inset distribution

    inset_distribution_pos = [0.64, 0.2, 0.09, 0.16]
    inset_distribution_ylabel = 'Density'
    inset_distribution_xlabel = 'Size-growth slope'

    ax_inset = fig.add_axes(inset_distribution_pos)
    sns.kdeplot(gdf_proj[col], ax=ax_inset, color='black', fill=True, alpha=0.2)
    ax_inset.axvline(x=gdf_proj[col].median(), color='black', linestyle='--', linewidth=0.5)
    style_inset_axes(ax=ax_inset, xlabel=inset_distribution_xlabel, ylabel=inset_distribution_ylabel)
    ax_inset.set_title('')
    ax_inset.set_yticks([])

    # Inset growth size


    inset_growth_size_pos = [0.2, 0.2, 0.12, 0.2]
    inset_growth_size_xlabel = 'Size (log population)'
    inset_growth_size_ylabel = 'Growth rate (log)'
    inset_label_text_font_size = 8
    inset_label_font_size = style_config['inset_label_font_size']

    ax_inset2 = fig.add_axes(inset_growth_size_pos)

    x_ = [4, 5, 6, 7]
    y_ = [0.04, 0.045, 0.07, 0.06]
    p = np.polyfit(x_, y_, 4)
    x_ = np.linspace(4, 7, 100)
    y_ = np.polyval(p, x_)
    ax_inset2.plot(x_, y_, color='black', linewidth=1)

    p_der = np.polyder(p)
    x_points = [4.5, 5.5, 6.5]
    shift_y = [-0.008, 0,  0.008]
    color_tangent = px.colors.qualitative.Plotly[1]
    for i, x_tan in enumerate(x_points):
        y_tan = np.polyval(p, x_tan)
        slope = np.polyval(p_der, x_tan)
        x_line = np.linspace(x_tan - 0.4, x_tan + 0.4, 2)
        y_line = slope * (x_line - x_tan) + y_tan
        ax_inset2.plot(x_line, y_line, color=color_tangent, linestyle='--', linewidth=1.5)
        shift_y_i = shift_y[i]
        if i == 2:
            ax_inset2.text(x_tan, y_tan + shift_y_i, f'Slope', fontsize=inset_label_text_font_size, ha='center', va='center', color=color_tangent)


    ax_inset2.annotate(
        text=r'Size-growth slope' + '=\naverage slope of this curve',
        xy=(5.5, 0.06),  # The point on the curve to point to
        xytext=(5.5, 0.12),    # Where the text is located
        xycoords='data',
        arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
        ha='center',
        va='center',
        fontsize=inset_label_font_size
    )
    # ax_inset2.text(5.5, 0.12, f'Size-growth slope =\naverage slope of this curve', fontsize=inset_font_size, fontfamily=font_family, ha='center', va='center')
    style_inset_axes(ax=ax_inset2, xlabel=inset_growth_size_xlabel, ylabel=inset_growth_size_ylabel)
    ax_inset2.set_yticks([])
    ax_inset2.set_xlim(4, 7)
    ax_inset2.set_ylim(0.02, 0.09)
    return fig, ax


def _cluster_bootstrap(data: pd.DataFrame, value_col: str, cluster_col: str, nboots: int) -> Tuple[float, float, float]:
    unique_clusters = data[cluster_col].unique()
    n_clusters = len(unique_clusters)
    bootstrap_means = []
    for i in range(nboots):
        sampled_cluster_ids = np.random.choice(unique_clusters, size=n_clusters, replace=True)
        bootstrap_sample = pd.concat([data[data[cluster_col] == c] for c in sampled_cluster_ids])
        bootstrap_means.append(np.mean(bootstrap_sample[value_col]))
    return np.median(bootstrap_means), np.percentile(bootstrap_means, 2.5), np.percentile(bootstrap_means, 97.5)

def _get_average_growth_rates_group_with_cis(average_growth_rates_group: pd.DataFrame, region_col: str, group_col: str, nboots: int) -> pd.DataFrame:
    regions = sorted(average_growth_rates_group[region_col].unique().tolist())
    groups = sorted(average_growth_rates_group[group_col].unique().tolist())
    average_growth_rates_group_with_cis = []

    for r in regions:
        for g in groups:
            average_growth_rates_group_region_df = average_growth_rates_group[(average_growth_rates_group[group_col] == g) & (average_growth_rates_group[region_col] == r)][['country', 'year', 'log_average_growth_group_demeaned']].copy().dropna()
            estimator = lambda x: x['log_average_growth_group_demeaned'].mean()
            # mean_value, ci_low, ci_high = clustered_boostrap_ci(estimator=estimator, df=average_growth_rates_group_region_df, cluster_col='country', nboots=nboots)
            mean_value, ci_low, ci_high = _cluster_bootstrap(data=average_growth_rates_group_region_df, value_col='log_average_growth_group_demeaned', cluster_col='country', nboots=nboots)
            average_growth_rates_group_with_cis.append({
                'region': r,
                'group': g,
                'mean': mean_value,
                'ci_low': ci_low,
                'ci_high': ci_high
            })

    average_growth_rates_group_with_cis = pd.DataFrame(average_growth_rates_group_with_cis)
    return average_growth_rates_group_with_cis

def _plot_average_growth_rates_group_barchart_by_region(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, nboots: int) -> Tuple[plt.Figure, plt.Axes]:
    y_axis_label = 'Growth advantage over national average \n' + r'($\log(g_{\text{group}} \ / \ g_{\text{national}})$)'

    region_col = 'region'
    group_col = 'group'

    group_to_label = {
        'largest_city': 'Largest city',
        'above_5M': 'Population\nabove 5M',
        'above_1M': 'Population\nabove 1M',
        'below_1M': 'Population\nbelow 1M',
    }

    world_average_growth_rate_group_with_cis = _get_average_growth_rates_group_with_cis(average_growth_rates_group=df, nboots=nboots, region_col=region_col, group_col=group_col)

    regions = sorted(world_average_growth_rate_group_with_cis[region_col].unique().tolist())  
    for i, r in enumerate(regions):
        for j, g in enumerate(group_to_label.keys()):
            average_growth_rates_group_region_with_cis = world_average_growth_rate_group_with_cis[(world_average_growth_rate_group_with_cis[region_col] == r) & (world_average_growth_rate_group_with_cis[group_col] == g)]
            mean_value, ci_low, ci_high = average_growth_rates_group_region_with_cis['mean'].values[0], average_growth_rates_group_region_with_cis['ci_low'].values[0], average_growth_rates_group_region_with_cis['ci_high'].values[0]
            color = region_colors[r]

            ax.bar(j + i * 0.2, mean_value, width=0.16, color=to_rgba(color, 0.2), edgecolor=color, linewidth=2)
            ax.plot([j + i * 0.2, j + i * 0.2], [ci_low, ci_high], color=color, linewidth=2)

    ax.axhline(0, color='grey', linewidth=2)
    ax.set_xticks(np.arange(len(group_to_label.keys())) + 0.2)
    ax.set_xticklabels(group_to_label.values())
    style_axes(ax=ax, xlabel='', ylabel=y_axis_label, title='Growth advantage')
    return fig, ax

def _plot_growth_size_curve_by_region(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'
    
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    region_col = 'region'

    regions = sorted(df_size_vs_growth_normalized[region_col].unique().tolist())
    for i, r in enumerate(regions):
        size_vs_growth_normalized_r = df_size_vs_growth_normalized[df_size_vs_growth_normalized[region_col] == r].copy()  
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=size_vs_growth_normalized_r, xaxis=x_axis, yaxis=y_axis, lam=lam)

        df_average_growth_r = df_average_growth[df_average_growth[region_col] == r].copy()
        average_log_growth_r = df_average_growth_r['log_average_growth'].mean()

        color = region_colors[r]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_r + y, ci_low=average_log_growth_r + ci_low, ci_high=average_log_growth_r + ci_high, color=color, label=r)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title='Size-growth curves')
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_average_size_growth_slope_with_borders()],
    group_name="figures"
)
def figure_2_map(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    apply_figure_theme()
    context.log.info(f"Creating figure 2: map")
    fig, ax = plt.subplots(figsize=(10, 4))
    figure_file_name = 'figure_2_map.png'

    rank_size_slopes_and_country_borders = read_postgis(engine=postgres.get_engine(), table=tables.names.world.figures.world_average_size_growth_slope_with_borders(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_world_map_with_colorbar(fig=fig, ax=ax, gdf=rank_size_slopes_and_country_borders)
    annotate_letter_label(axes=[ax], left_side=[True])

    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_average_growth_group(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(), TableNamesResource().names.world.figures.world_average_growth()],
    group_name="figures"
)
def figure_2_plots(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    apply_figure_theme()
    # Create a figure
    context.log.info(f"Creating figure 2: plots")
    figure_file_name = 'figure_2_plots.png'

    fig = plt.figure(figsize=(10, 4))
    gs = gridspec.GridSpec(1, 2, wspace=0.25)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])  

    nboots = 10
    engine = postgres.get_engine()
    world_average_growth_rate_group = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth_group(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_average_growth_rates_group_barchart_by_region(fig=fig, ax=ax1, df=world_average_growth_rate_group, nboots=nboots)

    world_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    world_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_growth_size_curve_by_region(fig=fig, ax=ax2, df_size_vs_growth_normalized=world_size_vs_growth_normalized, df_average_growth=world_average_growth)
    
    dummy_fig, dummy_ax = plt.subplots(figsize=(10, 4))
    annotate_letter_label(axes=[dummy_ax, ax1, ax2], left_side=[True, False, True])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)
    
