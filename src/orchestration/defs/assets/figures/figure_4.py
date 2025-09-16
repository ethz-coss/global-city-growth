# src/orchestration/defs/assets/figures/figure_4.py
import dagster as dg
from numpy import True_
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import plotly.express as px
import seaborn as sns
import os
from typing import Tuple, Dict

from sqlalchemy.sql.expression import true


from ...resources.resources import PostgresResource, TableNamesResource
from .figure_config import style_config, figure_dir, region_colors, MAIN_ANALYSIS_ID
from .figure_utils import fit_penalized_b_spline, materialize_image, rank_size_slope_by_year_with_cis, annotate_letter_label, get_mean_derivative_penalized_b_spline
from ..constants import constants



def _plot_rank_size_slope_by_urban_population_share(fig, ax, style_config, df: pd.DataFrame) -> None:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']

    x_axis = 'urban_population_share'
    y_axis = 'rank_size_slope'

    x_axis_label = 'Urban population share'
    y_axis_label = 'Dominance of large cities\n(Zipf exponent)'

    color = px.colors.qualitative.Plotly[0]

    lam = constants['PENALTY_SLOPE_SPLINE'] 

    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)

    ax.plot(x, y, color=color, linewidth=2)
    ax.fill_between(x, ci_low, ci_high, color=color, alpha=0.2)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)

    ax.set_title('Global cross-section', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax


def _get_rank_size_slope_by_year(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float) -> pd.DataFrame:
    years = sorted(df['year'].unique())
    slopes = []
    for y in years:
        df_y = df[df['year'] == y].copy()
        rank_size_slope = -1 * get_mean_derivative_penalized_b_spline(df=df_y, xaxis=xaxis, yaxis=yaxis, lam=lam)
        slopes.append({
            'year': y,
            'rank_size_slope': rank_size_slope,
        })

    slopes = pd.DataFrame(slopes)
    return slopes


def _plot_rank_size_slope_usa_kor(fig, ax, style_config, df_usa_rank_vs_size: pd.DataFrame, df_kor_rank_vs_size: pd.DataFrame, n_boots: int) -> None:
    lam = constants['PENALTY_RANK_SIZE_CURVE']

    df_usa = _get_rank_size_slope_by_year(df=df_usa_rank_vs_size, xaxis='log_rank', yaxis='log_population', lam=lam)
    df_kor = _get_rank_size_slope_by_year(df=df_kor_rank_vs_size, xaxis='log_rank', yaxis='log_population', lam=lam)


    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    inset_font_size = style_config['inset_font_size']
    inset_tick_font_size = style_config['inset_tick_font_size']
    title_font_size = style_config['title_font_size']

    x_axis = 'year'
    y_axis = 'rank_size_slope'

    x_axis_label = 'Year'
    y_axis_label = 'Dominance of large cities\n(Zipf exponent)'

    inset_y_axis_label = 'Zipf exponent'

    color = px.colors.qualitative.Plotly[0]

    ax.plot(df_usa[x_axis], df_usa[y_axis], color=color, linewidth=2, marker='o')

    ax_inset = fig.add_axes([0.35, 0.59, 0.1, 0.1])
    ax_inset.plot(df_kor[x_axis], df_kor[y_axis], color=color, linewidth=1, marker='o', markersize=2)

    ax_inset.set_xlabel(x_axis_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(inset_y_axis_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset.set_title('Korea', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax_inset)


    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('USA', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax


def _plot_rank_size_slope_by_region(fig, ax, style_config, region_colors: Dict[str, str], df: pd.DataFrame) -> None:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    region_col = 'region'

    x_axis = 'year'
    y_axis = 'rank_size_slope'

    x_axis_label = 'Year'
    y_axis_label = 'Dominance of large cities\n(Zipf exponent)'

    lam = constants['PENALTY_SLOPE_SPLINE']
    
    regions = sorted(df[region_col].unique())
    for r in regions:
        df_r = df[df[region_col] == r]
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_r, xaxis=x_axis, yaxis=y_axis, lam=lam)
        color = region_colors[r]
        ax.plot(x, y, color=color, linewidth=2, label=r)
        ax.fill_between(x, ci_low, ci_high, color=color, alpha=0.1)

    ax.plot([2025, 2025], [1.05, 1.27], color='grey', linewidth=1, linestyle='--')
    ax.annotate('Forecast', [2042, 1.25], fontsize=axis_font_size, fontfamily=font_family, ha='center')
    ax.annotate('Data', [2002, 1.25], fontsize=axis_font_size, fontfamily=font_family, ha='center')

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.legend(loc='lower right', fontsize=axis_font_size, frameon=False, ncol=2)
    sns.despine(ax=ax)
    return fig, ax


def _plot_rank_size_slope_by_urban_population_share_1950(fig, ax, style_config, df: pd.DataFrame) -> None:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    years = [1975, 2000, 2025]

    colors = [px.colors.qualitative.Plotly[7], px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    x_axis = 'urban_population_share_1950'
    y_axis = 'rank_size_slope'

    x_axis_label = 'Urban population share in 1950'
    y_axis_label = 'Dominance of large cities\n(Zipd exponent)'

    lam = constants['PENALTY_SLOPE_SPLINE']

    for i, yr in enumerate(years):
        df_y = df[df['year'] == yr]
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_y, xaxis=x_axis, yaxis=y_axis, lam=lam)
        ax.plot(x, y, color=colors[i], linewidth=2, label=yr)
        ax.fill_between(x, ci_low, ci_high, color=colors[i], alpha=0.1)


    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.legend(loc='lower right', fontsize=axis_font_size, frameon=False)
    sns.despine(ax=ax)
    return fig, ax
    

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_size_slopes_urbanization(), TableNamesResource().names.usa.figures.usa_rank_vs_size(), TableNamesResource().names.world.figures.world_rank_vs_size(), TableNamesResource().names.world.figures.world_rank_size_slopes_with_projections_and_regions()],
    group_name="figures"
)
def figure_4(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 4")
    figure_file_name = 'figure_4.png'
    figure_path = os.path.join(figure_dir, figure_file_name)

    fig, axes = plt.subplots(2, 2, figsize=(10, 10), gridspec_kw={'wspace': 0.25, 'hspace': 0.25})
    ax1, ax2, ax3, ax4 = axes.flatten()

    n_boots = 10
    usa_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_rank_vs_size()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())

    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_rank_vs_size()}
    WHERE country = 'KOR'
    AND analysis_id = {MAIN_ANALYSIS_ID}
    """
    kor_rank_vs_size = pd.read_sql(q, con=postgres.get_engine())
    _plot_rank_size_slope_usa_kor(fig=fig, ax=ax1, style_config=style_config, df_usa_rank_vs_size=usa_rank_vs_size, df_kor_rank_vs_size=kor_rank_vs_size, n_boots=n_boots)

    world_rank_size_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_size_slopes_urbanization()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_rank_size_slope_by_urban_population_share(fig=fig, ax=ax2, style_config=style_config, df=world_rank_size_slopes_urbanization)

    _plot_rank_size_slope_by_urban_population_share_1950(fig=fig, ax=ax3, style_config=style_config, df=world_rank_size_slopes_urbanization)

    q = f"""
    SELECT *
    FROM {tables.names.world.figures.world_rank_size_slopes_with_projections_and_regions()}
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """
    world_rank_size_slopes_with_projections = pd.read_sql(q, con=postgres.get_engine())
    context.log.info(f"World rank size slopes with projections: {world_rank_size_slopes_with_projections.shape}")
    context.log.info(f"World rank size slopes with projections: {world_rank_size_slopes_with_projections.head(10)}")

    _plot_rank_size_slope_by_region(fig=fig, ax=ax4, style_config=style_config, region_colors=region_colors, df=world_rank_size_slopes_with_projections)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4], left_side=[True, True, True, True], letter_label_font_size=style_config['letter_label_font_size'], font_family=style_config['font_family'])
    
    fig.savefig(figure_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return materialize_image(path=figure_path)