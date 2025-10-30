import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple
import numpy as np
import seaborn as sns

from ..figure_style import style_axes, style_config, annotate_letter_label
from ..figure_io import materialize_image, save_figure
from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_diagonal_correlation(fig, ax, df, x_axis, y_axis, title, x_axis_label, y_axis_label, diag_min, diag_max, alpha, annotation_font_size):
    diag = np.linspace(diag_min, diag_max, 100)
    ax.plot(diag, diag, color='black', linestyle='--', linewidth=2, label='Diagonal (45-degree line)')
    ax.scatter(df[x_axis], df[y_axis], color='grey', alpha=alpha)
    mean_error = np.mean(np.abs(df[y_axis] - df[x_axis]))
    ax.text(0.8, 0.05, f"Mean error: {mean_error:.3f}", ha='center', va='center', transform=ax.transAxes, fontsize=annotation_font_size)
    style_axes(ax, title=title, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax

def _plot_projection_vs_historical_urb_pop_share_cities_above_1m(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, is_country_plot: bool) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'm_t_data'
    y_axis = 'm_t_proj'

    y_axis_label = r'$m_t$'
    x_axis_label = r'$\hat{m_t}$'

    alpha = 0.4

    if is_country_plot:
        title = 'Country'
    else:
        title = 'Region'

    annotation_font_size = style_config['label_font_size']

    diag_min, diag_max = 0, 1
    _plot_diagonal_correlation(fig=fig, ax=ax, df=df, x_axis=x_axis, y_axis=y_axis, title=title, x_axis_label=x_axis_label, y_axis_label=y_axis_label, diag_min=diag_min, diag_max=diag_max, alpha=alpha, annotation_font_size=annotation_font_size)
    if is_country_plot:
        ax.legend(fontsize=annotation_font_size, frameon=False, ncol=1, bbox_to_anchor=(0.83, 0.95))
    return fig, ax


def _get_data_for_projection_vs_historical_urb_pop_share_cities_above_1m(postgres: PostgresResource, table_urb_pop_share_cities_above_1m_historical: str, table_urb_pop_share_cities_above_1m_projections: str, table_world_urban_population: str, table_world_regions: str) -> pd.DataFrame:
    q = f"""
    SELECT  country, 
            year, 
            analysis_id, 
            u.urban_population,
            r.region2 AS region,
            h.urban_population_share_cities_above_one_million AS m_t_data, 
            p.urban_population_share_cities_above_one_million AS m_t_proj
    FROM {table_urb_pop_share_cities_above_1m_historical} h
    JOIN {table_urb_pop_share_cities_above_1m_projections} p
    USING (analysis_id, country, year)
    JOIN {table_world_urban_population} u
    USING (country, year)
    JOIN {table_world_regions} r
    USING (country)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """ 
    df_country = pd.read_sql(q, con=postgres.get_engine())
    
    def estimator_func(col_name):
        return lambda x: np.sum(x[col_name] * x['urban_population']) / np.sum(x['urban_population'])
    df_region_data = df_country.groupby(['region', 'year']).apply(estimator_func('m_t_data')).reset_index().rename(columns={0: 'm_t_data'})
    df_region_proj = df_country.groupby(['region', 'year']).apply(estimator_func('m_t_proj')).reset_index().rename(columns={0: 'm_t_proj'})
    df_region = pd.merge(df_region_data, df_region_proj, on=['region', 'year'], how='inner')
    return df_country, df_region   

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_urb_pop_share_cities_above_1m_historical(), TableNamesResource().names.world.figures.world_urb_pop_share_cities_above_1m_projections(), TableNamesResource().names.world.si.world_urban_population(), TableNamesResource().names.world.sources.world_country_region()],
    group_name="si_figures"
)
def si_figure_projection_vs_historical_urb_pop_share_cities_above_1m(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting projection vs historical urban population share cities above 1m figure")

    figure_file_name = 'si_figure_projection_vs_historical_urb_pop_share_cities_above_1m.png'
    fig, axes = plt.subplots(1, 2, figsize=(10, 5), gridspec_kw={'wspace': 0.25})
    ax1, ax2 = axes.flatten()
    
    df_country, df_region = _get_data_for_projection_vs_historical_urb_pop_share_cities_above_1m(postgres=postgres, table_urb_pop_share_cities_above_1m_historical=tables.names.world.figures.world_urb_pop_share_cities_above_1m_historical(), table_urb_pop_share_cities_above_1m_projections=tables.names.world.figures.world_urb_pop_share_cities_above_1m_projections(), table_world_urban_population=tables.names.world.si.world_urban_population(), table_world_regions=tables.names.world.sources.world_country_region())

    _plot_projection_vs_historical_urb_pop_share_cities_above_1m(fig=fig, ax=ax1, df=df_country, is_country_plot=True)
    _plot_projection_vs_historical_urb_pop_share_cities_above_1m(fig=fig, ax=ax2, df=df_region, is_country_plot=False)

    annotate_letter_label(axes=[ax1, ax2], left_side=[True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)
    return materialize_image(figure_file_name=figure_file_name, si=True)

