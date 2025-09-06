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

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_config import style_config, region_colors, figure_dir, MAIN_ANALYSIS_ID
from .figure_utils import cluster_bootstrap, fit_penalized_b_spline, materialize_image
from ..constants import constants

def _create_bicolor_cmap(cmap_neg: str, cmap_pos: str, midpoint_frac: float, name: str = 'bicolor_cmap', N: int = 256) -> mcolors.LinearSegmentedColormap:
    """
    Combines two colormaps into a single new one.

    Args:
        cmap_neg (str): Name of the colormap for the range below the midpoint.
        cmap_pos (str): Name of the colormap for the range above the midpoint.
        midpoint_frac (float): The center point as a fraction (0 to 1).
        name (str): The name for the new colormap.
        N (int): The number of colors in the new colormap.

    Returns:
        matplotlib.colors.LinearSegmentedColormap: The new combined colormap.
    """
    # Get the colormap objects
    cmap_neg_obj = plt.get_cmap(cmap_neg)
    cmap_pos_obj = plt.get_cmap(cmap_pos)
    print(midpoint_frac)

    # Determine the number of colors for each part
    n_neg = int(np.round(N * midpoint_frac))
    n_pos = N - n_neg

    # Sample colors from each colormap
    neg_colors = cmap_neg_obj(np.linspace(1 - midpoint_frac, 1, n_neg))
    pos_colors = cmap_pos_obj(np.linspace(0.0, 1 - midpoint_frac, n_pos))
    
    # Combine the color lists
    all_colors = np.vstack((neg_colors, pos_colors))

    # Create the new colormap
    return mcolors.LinearSegmentedColormap.from_list(name, all_colors)


def _plot_world_map_with_colorbar(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], gdf: gpd.GeoDataFrame) -> Tuple[plt.Figure, plt.Axes]:
    # Parameters
    col = 'size_growth_slope'
    midpoint = 0
    
    missing_hatch = '///'
    missing_color = 'white'
    projection_epsg = 4087

    font_family = style_config['font_family']
    inset_font_size = style_config['inset_font_size'] 
    inset_tick_font_size = style_config['inset_tick_font_size']
    inset_text_label_font_size = style_config['inset_text_label_font_size']
    colorbar_label_fontsize = style_config['colorbar_label_font_size']

    colorbar_pos = [0.4, 0.13, 0.2, 0.025]
    colorbar_label_pos = 'top'
    colorbar_label = 'Size-growth slope'

    inset_distribution_pos = [0.66, 0.2, 0.1, 0.2]
    inset_distribution_ylabel = 'Density'
    inset_distribution_xlabel = 'Size-growth slope'

    inset_growth_size_pos = [0.1, 0.2, 0.15, 0.2]
    inset_growth_size_xlabel = 'Size (log population)'
    inset_growth_size_ylabel = 'Growth rate (log)'

    # Map 
    
    gdf_proj = gdf.to_crs(epsg=projection_epsg)
    vmin, vmax = np.nanmin(gdf_proj[col]), np.nanmax(gdf_proj[col])
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    midpoint_frac = (midpoint - vmin) / (vmax - vmin)
    rescaled_cmap = _create_bicolor_cmap(cmap_neg='Greys_r', cmap_pos='Blues', midpoint_frac=midpoint_frac)


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
    cax.set_xlabel(colorbar_label, fontsize=colorbar_label_fontsize, fontfamily=font_family)
    cax.xaxis.set_label_position(colorbar_label_pos)


    # Inset distribution

    ax_inset = fig.add_axes(inset_distribution_pos)
    sns.kdeplot(gdf_proj[col], ax=ax_inset, color='black', fill=True, alpha=0.2)
    ax_inset.axvline(x=gdf_proj[col].median(), color='black', linestyle='--', linewidth=0.5)
    ax_inset.text(gdf_proj[col].median() + 0.01, 25, f'Median: {gdf_proj[col].median():.2f}', fontsize=inset_text_label_font_size, ha='center', va='center', fontfamily=font_family)
    ax_inset.set_xlabel(inset_distribution_xlabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(inset_distribution_ylabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset.set_title('')
    ax_inset.set_yticks([])
    sns.despine(ax=ax_inset)

    # Inset growth size

    ax_inset2 = fig.add_axes(inset_growth_size_pos)

    x_ = [4, 5, 6, 7]
    y_ = [0.04, 0.045, 0.07, 0.06]
    p = np.polyfit(x_, y_, 4)
    x_ = np.linspace(4, 7, 100)
    y_ = np.polyval(p, x_)
    ax_inset2.plot(x_, y_, color='black', linewidth=1)

    p_der = np.polyder(p)
    x_points = [4.5, 6.5]
    shift_y = [-0.008, 0.008]
    color_tangent = px.colors.qualitative.Plotly[1]
    for i, x_tan in enumerate(x_points):
        y_tan = np.polyval(p, x_tan)
        slope = np.polyval(p_der, x_tan)
        x_line = np.linspace(x_tan - 0.5, x_tan + 0.5, 2)
        y_line = slope * (x_line - x_tan) + y_tan
        ax_inset2.plot(x_line, y_line, color=color_tangent, linestyle='--', linewidth=1.5)
        shift_y_i = shift_y[i]
        ax_inset2.text(x_tan, y_tan + shift_y_i, f'Slope', fontsize=inset_text_label_font_size, ha='center', va='center', fontfamily=font_family, color=color_tangent)


    ax_inset2.annotate(
        text=r'Size-growth slope' + '=\naverage slope of this curve',
        xy=(5.5, 0.06),  # The point on the curve to point to
        xytext=(5.5, 0.12),    # Where the text is located
        xycoords='data',
        arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
        ha='center',
        va='center'
    )
    # ax_inset2.text(5.5, 0.12, f'Size-growth slope =\naverage slope of this curve', fontsize=inset_font_size, fontfamily=font_family, ha='center', va='center')
    ax_inset2.set_xlabel(inset_growth_size_xlabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset2.set_ylabel(inset_growth_size_ylabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset2.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset2.set_yticks([])
    ax_inset2.set_xlim(4, 7)
    ax_inset2.set_ylim(0.02, 0.09)
    sns.despine(ax=ax_inset2)
    return fig, ax

def _get_average_growth_rates_group_with_cis(average_growth_rates_group: pd.DataFrame, region_col: str, group_col: str, nboots: int) -> pd.DataFrame:
    regions = sorted(average_growth_rates_group[region_col].unique().tolist())
    groups = sorted(average_growth_rates_group[group_col].unique().tolist())
    average_growth_rates_group_with_cis = []

    for r in regions:
        for g in groups:
            average_growth_rates_group_region_df = average_growth_rates_group[(average_growth_rates_group[group_col] == g) & (average_growth_rates_group[region_col] == r)][['country', 'year', 'log_average_growth_group_demeaned']].copy().dropna()
            mean_value, ci_low, ci_high = cluster_bootstrap(data=average_growth_rates_group_region_df, value_col='log_average_growth_group_demeaned', cluster_col='country', nboots=nboots)
            average_growth_rates_group_with_cis.append({
                'region': r,
                'group': g,
                'mean': mean_value,
                'ci_low': ci_low,
                'ci_high': ci_high
            })

    average_growth_rates_group_with_cis = pd.DataFrame(average_growth_rates_group_with_cis)
    return average_growth_rates_group_with_cis

def _plot_average_growth_rates_group_barchart_by_region(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: Dict[str, str], df: pd.DataFrame, nboots: int) -> Tuple[plt.Figure, plt.Axes]:
    axis_font_size = style_config['axis_font_size']
    title_font_size = style_config['title_font_size']
    font_family = style_config['font_family']
    tick_font_size = style_config['tick_font_size']

    x_axis_label = 'City sub-group'
    y_axis_label = 'Sub-group growth advantage\nover national average'

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
            color = colors[r]

            ax.bar(j + i * 0.2, mean_value, width=0.16, color=to_rgba(color, 0.2), edgecolor=color, linewidth=2)
            ax.plot([j + i * 0.2, j + i * 0.2], [ci_low, ci_high], color=color, linewidth=2)
            ax.bar(j + i * 0.2, mean_value, width=0.16, color=to_rgba(color, 0.2), edgecolor=color, linewidth=2)

    ax.axhline(0, color='grey', linewidth=2)
    ax.set_xticks(np.arange(len(group_to_label.keys())) + 0.2)
    ax.set_xticklabels(group_to_label.values(), fontsize=tick_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('Growth advantage', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax

def _plot_growth_size_curve_by_region(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: Dict[str, str], df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    title_font_size = style_config['title_font_size']
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size'] 
    
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

        color = colors[r]
        ax.plot(x, average_log_growth_r + y, color=color, label=r)
        ax.fill_between(x, average_log_growth_r + ci_low, average_log_growth_r + ci_high, color=color, alpha=0.2)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('Size-growth curves', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes()],
    group_name="figures"
)
def figure_2_map(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info(f"Creating figure 2: map")

    fig, ax = plt.subplots(figsize=(10, 4))
    figure_file_name = 'figure_2_map.png'
    figure_path = os.path.join(figure_dir, figure_file_name)
    q = f"""
    SELECT * 
    FROM {tables.names.world.figures.world_average_size_growth_slope_with_borders()}
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """
    rank_size_slopes_and_country_borders = gpd.read_postgis(q, con=postgres.get_engine())
    _plot_world_map_with_colorbar(fig=fig, ax=ax, style_config=style_config, gdf=rank_size_slopes_and_country_borders)
    fig.tight_layout() # TODO: Move to bbox inches tight
    fig.savefig(figure_path, dpi=300)
    plt.close(fig)

    return materialize_image(path=figure_path)


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_average_growth_group(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(), TableNamesResource().names.world.figures.world_average_growth()],
    group_name="figures"
)
def figure_2_plots(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 2: plots")
    figure_file_name = 'figure_2_plots.png'
    figure_path = os.path.join(figure_dir, figure_file_name)

    fig = plt.figure(figsize=(10, 4))
    gs = gridspec.GridSpec(1, 2, wspace=0.25)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])  

    nboots = 10
    world_average_growth_rate_group = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_average_growth_group()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())    
    _plot_average_growth_rates_group_barchart_by_region(fig=fig, ax=ax1, style_config=style_config, colors=region_colors, df=world_average_growth_rate_group, nboots=nboots)

    world_size_vs_growth_normalized = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth_normalized()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    world_average_growth = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_average_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_growth_size_curve_by_region(fig=fig, ax=ax2, style_config=style_config, colors=region_colors, df_size_vs_growth_normalized=world_size_vs_growth_normalized, df_average_growth=world_average_growth)
    
    fig.savefig(figure_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return materialize_image(path=figure_path)
    
