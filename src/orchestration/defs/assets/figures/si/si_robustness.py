# src/orchestration/defs/assets/figures/tables.py
import dagster as dg
import pandas as pd
import os
from typing import Tuple, List
import matplotlib.pyplot as plt
from matplotlib import gridspec
from typing import Dict, Any
import seaborn as sns


from ....resources.resources import PostgresResource, TableNamesResource
from ..figure_config import table_si_dir, MAIN_ANALYSIS_ID, style_config, figure_si_dir
from ..tables import make_table_2
from ..figure_utils import fit_penalized_b_spline, size_growth_slope_by_year_with_cis, rank_size_slope_by_year_with_cis, materialize_image
from ...constants import constants


def _plot_size_growth_slope_usa_by_year_and_analysis_id(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, n_boots: int, map_analysis_id_to_urban_threshold: Dict[int, int], epochs: List[str]) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'year'
    y_axis = 'size_growth_slope'
    
    x_axis_label = 'Year'
    y_axis_label = 'Size-growth slope'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    analysis_ids = sorted(df_size_vs_growth['analysis_id'].unique())

    colors = ['red', 'blue', 'green']

    for i,a in enumerate(analysis_ids):
        df_size_vs_growth_a = df_size_vs_growth[df_size_vs_growth['analysis_id'] == a].copy()
        size_growth_slope_cis_a = size_growth_slope_by_year_with_cis(df=df_size_vs_growth_a, xaxis='log_population', yaxis='log_growth', lam=lam, n_boots=n_boots)
        ax.plot(size_growth_slope_cis_a[x_axis], size_growth_slope_cis_a[y_axis], color=colors[i], linewidth=2, marker='o')
        ax.fill_between(size_growth_slope_cis_a[x_axis], size_growth_slope_cis_a['ci_low'], size_growth_slope_cis_a['ci_high'], color=colors[i], alpha=0.1)


    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    sns.despine(ax=ax)
    return fig, ax


def _plot_rank_size_slope_usa_by_year_and_analysis_id(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'year'
    y_axis = 'rank_size_slope'
    
    x_axis_label = 'Year'
    y_axis_label = 'Zipf exponent'

    lam = constants['PENALTY_RANK_SIZE_CURVE']

    analysis_ids = sorted(df_size_vs_growth['analysis_id'].unique())

    colors = ['red', 'blue', 'green']

    for i,a in enumerate(analysis_ids):
        df_size_vs_growth_a = df_size_vs_growth[df_size_vs_growth['analysis_id'] == a].copy()
        rank_size_slope_cis_a = rank_size_slope_by_year_with_cis(df=df_size_vs_growth_a, xaxis='log_rank', yaxis='log_population', lam=lam, n_boots=n_boots)
        ax.plot(rank_size_slope_cis_a[x_axis], rank_size_slope_cis_a[y_axis], color=colors[i], linewidth=2, marker='o')
        ax.fill_between(rank_size_slope_cis_a[x_axis], rank_size_slope_cis_a['ci_low'], rank_size_slope_cis_a['ci_high'], color=colors[i], alpha=0.1)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    sns.despine(ax=ax)
    return fig, ax


def _plot_city_counts_usa_by_analysis_id(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, map_analysis_id_to_urban_threshold: Dict[int, int], epochs: List[str]) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'urban_threshold'
    y_axis = 'ratio'

    x_axis_label = 'Urban threshold'
    y_axis_label = 'Share of cities remaining in the sample'

    city_counts_main_analysis = df_size_vs_growth[df_size_vs_growth['analysis_id'] == 1].groupby('epoch')['cluster_id'].count().reset_index().rename(columns={'cluster_id': 'num_cities'})
    city_counts_all_analyses = df_size_vs_growth.groupby(['epoch', 'analysis_id'])['cluster_id'].count().reset_index().rename(columns={'cluster_id': 'num_cities'})
    city_counts_ratios = city_counts_all_analyses.merge(city_counts_main_analysis, on='epoch', how='inner', suffixes=('', '_main'))
    city_counts_ratios['ratio'] = city_counts_ratios['num_cities'] / city_counts_ratios['num_cities_main']
    city_counts_ratios['urban_threshold'] = city_counts_ratios['analysis_id'].map(map_analysis_id_to_urban_threshold)

    line_styles = ['-', '--', ':']
    for i, e in enumerate(epochs):
        city_counts_ratios_e = city_counts_ratios[city_counts_ratios['epoch'] == e].copy()
        ax.plot(city_counts_ratios_e[x_axis], city_counts_ratios_e[y_axis], color='black', linewidth=2, linestyle=line_styles[i], label=e)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.legend(fontsize=axis_font_size, frameon=False, loc='upper right')
    ax.set_ylim(0, 1)
    sns.despine(ax=ax)
    return fig, ax

def _plot_size_growth_slopes_usa_for_mixed_analysis(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'year'
    y_axis = 'size_growth_slope'
    
    x_axis_label = 'Year'
    y_axis_label = 'Size-growth slope'

    lam_size_growth_slope = constants['PENALTY_SIZE_GROWTH_CURVE']
    color = 'black'
    
    size_growth_slope = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam_size_growth_slope, n_boots=n_boots)

    ax.plot(size_growth_slope[x_axis], size_growth_slope[y_axis], color=color, linewidth=2, marker='o')
    ax.fill_between(size_growth_slope[x_axis], size_growth_slope['ci_low'], size_growth_slope['ci_high'], color=color, alpha=0.2)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    sns.despine(ax=ax)
    return fig, ax

def _plot_rank_size_slopes_usa_for_mixed_analysis(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], df_rank_vs_size: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']

    x_axis = 'year'
    y_axis = 'rank_size_slope'
    
    x_axis_label = 'Year'
    y_axis_label = 'Zipf exponent'

    lam_rank_size_slope = constants['PENALTY_RANK_SIZE_CURVE']
    color = 'black'
    
    rank_size_slope = rank_size_slope_by_year_with_cis(df=df_rank_vs_size, xaxis='log_rank', yaxis='log_population', lam=lam_rank_size_slope, n_boots=n_boots)
    
    ax.plot(rank_size_slope[x_axis], rank_size_slope[y_axis], color=color, linewidth=2, marker='o')
    ax.fill_between(rank_size_slope[x_axis], rank_size_slope['ci_low'], rank_size_slope['ci_high'], color=color, alpha=0.2)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title('Mixed threshold analysis', fontsize=title_font_size, fontfamily=font_family)
    sns.despine(ax=ax)
    return fig, ax

def _plot_size_growth_curve_usa_by_analysis_id(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], show_legend: bool, title: str, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, map_analysis_id_to_urban_threshold: Dict[int, int])-> Tuple[plt.Figure, plt.Axes]:
    title_font_size = style_config['title_font_size']
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'
    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    colors = ['red', 'blue', 'green']

    analysis_ids = sorted(df_size_vs_growth_normalized['analysis_id'].unique())

    for i, a in enumerate(analysis_ids):
        pop_growth_a = df_size_vs_growth_normalized[df_size_vs_growth_normalized['analysis_id'] == a].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_a, xaxis=x_axis, yaxis=y_axis, lam=lam)
        average_log_growth_a = df_average_growth[df_average_growth['analysis_id'] == a]['log_average_growth'].mean()

        color = colors[i]
        ax.plot(x, average_log_growth_a + y, color=color, label=f'Urban threshold: {map_analysis_id_to_urban_threshold[a]}', linewidth=2)
        ax.fill_between(x, average_log_growth_a + ci_low, average_log_growth_a + ci_high, color=color, alpha=0.1)

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_title(title, fontsize=title_font_size, fontfamily=font_family)
    if show_legend:
        ax.legend(fontsize=axis_font_size, frameon=False, bbox_to_anchor=(1.6, -0.2), ncol=3)
    sns.despine(ax=ax)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_urbanization(), TableNamesResource().names.world.figures.world_rank_size_slopes_urbanization(), TableNamesResource().names.other.analysis_parameters()],
    group_name="supplementary_information"
)
def world_robustness_tables(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Creating world robustness tables")
    params = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = params[params['robustness_for_dataset'] == 'world']['analysis_id'].tolist()

    for analysis_id in analysis_ids:
        table_file_name = f'table_2_robustness_{analysis_id}.txt'
        table_path = os.path.join(table_si_dir, table_file_name)

        context.log.info(f"Creating table for analysis id {analysis_id}")
        world_size_growth_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_growth_slopes_urbanization()} WHERE analysis_id = {analysis_id}", con=postgres.get_engine())
        world_rank_size_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_size_slopes_urbanization()} WHERE analysis_id = {analysis_id}", con=postgres.get_engine())  
        latex_table = make_table_2(df_size_growth_slopes=world_size_growth_slopes_urbanization, df_rank_size_slopes=world_rank_size_slopes_urbanization)

        with open(table_path, 'w') as f:
            f.write(latex_table)

    return dg.MaterializeResult(
        metadata={
            "path": str(table_si_dir),
            "num_records_processed": len(analysis_ids),
        }
    )


@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(), TableNamesResource().names.usa.figures.usa_rank_vs_size(), TableNamesResource().names.usa.figures.usa_average_growth(), TableNamesResource().names.usa.figures.usa_year_epoch()],
    group_name="supplementary_information"
)
def usa_robustness_figure(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Creating usa robustness figure")

    analysis_id_50 = MAIN_ANALYSIS_ID
    analysis_id_100 = 9
    analysis_id_200 = 10
    map_analysis_id_to_urban_threshold = {analysis_id_50: 50, analysis_id_100: 100, analysis_id_200: 200}
    e1, e2, e3 = '1850-1880', '1900-1940', '1990-2020'
    epochs = [e1, e2, e3]
    n_boots = 100 # TODO: Change to 100


    # Prepare data
    analysis_ids = [analysis_id_50, analysis_id_100, analysis_id_200]
    q = f"""
    SELECT * 
    FROM {tables.names.usa.figures.usa_size_vs_growth()} 
    JOIN {tables.names.usa.figures.usa_year_epoch()}
    USING (year)
    WHERE analysis_id IN {tuple(analysis_ids)}"""
    size_vs_growth = pd.read_sql(q, con=postgres.get_engine())
    size_vs_growth_normalized = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_size_vs_growth_normalized()} WHERE analysis_id IN {tuple(analysis_ids)}", con=postgres.get_engine())
    
    q = f"""
    SELECT * 
    FROM {tables.names.usa.figures.usa_rank_vs_size()}
    JOIN {tables.names.usa.figures.usa_year_epoch()}
    USING (year)
    WHERE analysis_id IN {tuple(analysis_ids)}"""
    rank_vs_size = pd.read_sql(q, con=postgres.get_engine())
    
    average_growth = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_average_growth()} WHERE analysis_id IN {tuple(analysis_ids)}", con=postgres.get_engine())

    mixed_size_vs_growth = pd.concat([size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_50) & (size_vs_growth['epoch'] == e1)], size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_100) & (size_vs_growth['epoch'] == e2)], size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_200) & (size_vs_growth['epoch'] == e3)]])
    mixed_rank_vs_size = pd.concat([rank_vs_size[(rank_vs_size['analysis_id'] == analysis_id_50) & (rank_vs_size['epoch'] == e1)], rank_vs_size[(rank_vs_size['analysis_id'] == analysis_id_100) & (rank_vs_size['epoch'] == e2)], rank_vs_size[(rank_vs_size['analysis_id'] == analysis_id_200) & (rank_vs_size['epoch'] == e3)]])


    # Create figure
    fig = plt.figure(figsize=(20, 10))
    gs1 = gridspec.GridSpec(2, 4, wspace=0.25, hspace=0.4)
    ax1 = fig.add_subplot(gs1[0])
    ax2 = fig.add_subplot(gs1[1], sharey=ax1)
    ax3 = fig.add_subplot(gs1[2], sharey=ax1)
    ax4 = fig.add_subplot(gs1[3])
    ax5 = fig.add_subplot(gs1[4])
    ax6 = fig.add_subplot(gs1[5])
    ax7 = fig.add_subplot(gs1[6])
    ax8 = fig.add_subplot(gs1[7])

    axes = [ax1, ax2, ax3]
    for i, e in enumerate(epochs):
        size_vs_growth_normalized_e = size_vs_growth_normalized[size_vs_growth_normalized['epoch'] == e]
        average_growth_e = average_growth[average_growth['epoch'] == e]
        if i == 1:
            _plot_size_growth_curve_usa_by_analysis_id(fig=fig, ax=axes[i], style_config=style_config, show_legend=True, title=e, df_size_vs_growth_normalized=size_vs_growth_normalized_e, df_average_growth=average_growth_e, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold)
        else:
            _plot_size_growth_curve_usa_by_analysis_id(fig=fig, ax=axes[i], style_config=style_config, show_legend=False, title=e, df_size_vs_growth_normalized=size_vs_growth_normalized_e, df_average_growth=average_growth_e, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold)

    _plot_city_counts_usa_by_analysis_id(fig=fig, ax=ax4, style_config=style_config, df_size_vs_growth=size_vs_growth, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold, epochs=epochs)

    _plot_size_growth_slope_usa_by_year_and_analysis_id(fig=fig, ax=ax5, style_config=style_config, df_size_vs_growth=size_vs_growth, n_boots=n_boots, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold, epochs=epochs)
    _plot_rank_size_slope_usa_by_year_and_analysis_id(fig=fig, ax=ax6, style_config=style_config, df_size_vs_growth=rank_vs_size, n_boots=n_boots)

    _plot_size_growth_slopes_usa_for_mixed_analysis(fig=fig, ax=ax7, style_config=style_config, df_size_vs_growth=mixed_size_vs_growth, n_boots=n_boots)
    _plot_rank_size_slopes_usa_for_mixed_analysis(fig=fig, ax=ax8, style_config=style_config, df_rank_vs_size=mixed_rank_vs_size, n_boots=n_boots)
            
    figure_file_name = 'usa_robustness_figure.png'
    figure_path = os.path.join(figure_si_dir, figure_file_name)
    fig.savefig(figure_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return materialize_image(path=figure_path)