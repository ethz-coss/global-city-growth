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
from ..figure_style import style_axes, style_config, annotate_letter_label
from ..figure_io import materialize_image, MAIN_ANALYSIS_ID, save_figure, read_pandas, save_latex_table, materialize_table
from ..figure_stats import fit_penalized_b_spline, size_growth_slope_by_year_with_cis
from ...constants import constants
from ..tables import make_table_2

def _plot_size_growth_curve_usa_by_analysis_id(fig: plt.Figure, ax: plt.Axes, show_legend: bool, title: str, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, map_analysis_id_to_urban_threshold: Dict[int, int])-> Tuple[plt.Figure, plt.Axes]:
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

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)
    if show_legend:
        ax.legend(fontsize=style_config['label_font_size'], frameon=False, bbox_to_anchor=(1.6, -0.2), ncol=3)
    return fig, ax

def _plot_size_growth_slope_usa_by_year_and_analysis_id(fig: plt.Figure, ax: plt.Axes,  df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
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

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax


def _plot_city_counts_usa_by_analysis_id(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, map_analysis_id_to_urban_threshold: Dict[int, int], epochs: List[str]) -> Tuple[plt.Figure, plt.Axes]:
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

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, legend_loc='lower left')
    ax.set_ylim(0, 1)
    return fig, ax

def _plot_size_growth_slopes_usa_for_mixed_analysis(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'year'
    y_axis = 'size_growth_slope'
    
    x_axis_label = 'Year'
    y_axis_label = 'Size-growth slope'

    lam_size_growth_slope = constants['PENALTY_SIZE_GROWTH_CURVE']
    color = 'black'
    
    size_growth_slope = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam_size_growth_slope, n_boots=n_boots)

    ax.plot(size_growth_slope[x_axis], size_growth_slope[y_axis], color=color, linewidth=2, marker='o')
    ax.fill_between(size_growth_slope[x_axis], size_growth_slope['ci_low'], size_growth_slope['ci_high'], color=color, alpha=0.2)

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title='Mixed threshold analysis')
    return fig, ax


def _get_data_size_vs_growth(postgres: PostgresResource, table_size_vs_growth: str, table_size_vs_growth_normalized: str, table_year_epoch: str, analysis_ids: List[int]) -> pd.DataFrame:
    q = f"""
    SELECT * 
    FROM {table_size_vs_growth} 
    JOIN {table_year_epoch}
    USING (year)
    WHERE analysis_id IN {tuple(analysis_ids)}"""
    size_vs_growth = pd.read_sql(q, con=postgres.get_engine())
    size_vs_growth_normalized = pd.read_sql(f"SELECT * FROM {table_size_vs_growth_normalized} WHERE analysis_id IN {tuple(analysis_ids)}", con=postgres.get_engine())
    return size_vs_growth, size_vs_growth_normalized

@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(),TableNamesResource().names.usa.figures.usa_year_epoch(), TableNamesResource().names.usa.figures.usa_average_growth()],
    group_name="supplementary_information"
)
def si_figure_usa_robustness(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Creating usa robustness figure")
    figure_file_name = 'si_figure_usa_robustness.png'

    analysis_id_50 = MAIN_ANALYSIS_ID
    analysis_id_100 = 9
    analysis_id_200 = 10
    map_analysis_id_to_urban_threshold = {analysis_id_50: 50, analysis_id_100: 100, analysis_id_200: 200}
    e1, e2, e3 = '1850-1880', '1900-1940', '1990-2020'
    epochs = [e1, e2, e3]
    n_boots = 100


    # Prepare data
    analysis_ids = [analysis_id_50, analysis_id_100, analysis_id_200]
    size_vs_growth, size_vs_growth_normalized = _get_data_size_vs_growth(postgres=postgres, 
                                                                        table_size_vs_growth=tables.names.usa.figures.usa_size_vs_growth(), 
                                                                        table_size_vs_growth_normalized=tables.names.usa.figures.usa_size_vs_growth_normalized(), 
                                                                        table_year_epoch=tables.names.usa.figures.usa_year_epoch(), 
                                                                        analysis_ids=analysis_ids)

    average_growth = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_average_growth()} WHERE analysis_id IN {tuple(analysis_ids)}", con=postgres.get_engine())

    size_vs_growth_e1 = size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_50) & (size_vs_growth['epoch'] == e1)]
    size_vs_growth_e2 = size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_100) & (size_vs_growth['epoch'] == e2)]
    size_vs_growth_e3 = size_vs_growth[(size_vs_growth['analysis_id'] == analysis_id_200) & (size_vs_growth['epoch'] == e3)]
    mixed_size_vs_growth = pd.concat([size_vs_growth_e1, size_vs_growth_e2, size_vs_growth_e3])

    # Create figure
    fig = plt.figure(figsize=(15, 10))
    gs1 = gridspec.GridSpec(2, 3, wspace=0.25, hspace=0.4)
    ax1 = fig.add_subplot(gs1[0])
    ax2 = fig.add_subplot(gs1[1], sharey=ax1)
    ax3 = fig.add_subplot(gs1[2], sharey=ax1)
    ax4 = fig.add_subplot(gs1[3])
    ax5 = fig.add_subplot(gs1[4])
    ax6 = fig.add_subplot(gs1[5])

    axes = [ax1, ax2, ax3]
    for i, e in enumerate(epochs):
        size_vs_growth_normalized_e = size_vs_growth_normalized[size_vs_growth_normalized['epoch'] == e]
        average_growth_e = average_growth[average_growth['epoch'] == e]
        _plot_size_growth_curve_usa_by_analysis_id(fig=fig, ax=axes[i], show_legend=i == 1, title=e, df_size_vs_growth_normalized=size_vs_growth_normalized_e, df_average_growth=average_growth_e, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold)

    _plot_city_counts_usa_by_analysis_id(fig=fig, ax=ax4, df_size_vs_growth=size_vs_growth, map_analysis_id_to_urban_threshold=map_analysis_id_to_urban_threshold, epochs=epochs)

    _plot_size_growth_slope_usa_by_year_and_analysis_id(fig=fig, ax=ax5, df_size_vs_growth=size_vs_growth, n_boots=n_boots)
    _plot_size_growth_slopes_usa_for_mixed_analysis(fig=fig, ax=ax6, df_size_vs_growth=mixed_size_vs_growth, n_boots=n_boots)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5, ax6], left_side=[True, True, True, False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)
    return materialize_image(figure_file_name=figure_file_name, si=True)


@dg.asset(
    deps=[TableNamesResource().names.other.analysis_parameters(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization()],
    group_name="supplementary_information"
)
def si_tables_world_robustness(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Creating world robustness tables")
    params = pd.read_sql(f"SELECT * FROM {tables.names.other.analysis_parameters()}", con=postgres.get_engine())
    analysis_ids = params[params['robustness_for_dataset'] == 'world']['analysis_id'].tolist()

    for analysis_id in analysis_ids:
        table_file_name = f'table_2_robustness_{analysis_id}.txt'
        world_size_growth_slopes_urbanization = read_pandas(engine=postgres.get_engine(), table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(), analysis_id=analysis_id)
        latex_table = make_table_2(df_size_growth_slopes=world_size_growth_slopes_urbanization)

        save_latex_table(table=latex_table, table_file_name=table_file_name, si=True)
       
       
    return materialize_table(table_file_name='table_2_robustness.txt')