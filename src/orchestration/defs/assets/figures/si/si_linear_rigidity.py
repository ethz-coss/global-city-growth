# src/orchestration/defs/assets/figures/si/si_linear_rigidity.py

import numpy as np
import pandas as pd
from scipy.stats import norm
import statsmodels.formula.api as smf
from pygam import LinearGAM, s
import dagster as dg
from typing import Dict, Any, Tuple, List
import plotly.express as px
import seaborn as sns
import os
import matplotlib.pyplot as plt
from matplotlib import gridspec

from ....resources.resources import PostgresResource, TableNamesResource
from ..figure_config import MAIN_ANALYSIS_ID, style_config, figure_si_dir
from ..figure_utils import fit_penalized_b_spline, materialize_image, get_mean_derivative_penalized_b_spline
from ...constants import constants


def _calculate_gaussian_loglik(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    n = len(y_true)
    rss = np.sum((y_true - y_pred)**2)
    sigma2_mle = rss / n
    log_likelihood = np.sum(norm.logpdf(y_true, loc=y_pred, scale=np.sqrt(sigma2_mle)))
    return log_likelihood

def _test_linearity_of_size_growth_curve(df: pd.DataFrame, x_axis: str, y_axis: str, n_boots: int, lam: float) -> bool:
    df = df[[x_axis, y_axis]].copy()
    n = df.shape[0]
    lin = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()

    X = df[[x_axis]].values
    y = df[y_axis].values
    spline = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X, y)

    loglik_lin = _calculate_gaussian_loglik(y_true=y, y_pred=lin.predict(df))
    loglik_spline = _calculate_gaussian_loglik(y_true=y, y_pred=spline.predict(X))
    D_true = 2 * (loglik_spline - loglik_lin)

    yhat0 = lin.params['Intercept'] + lin.params[x_axis] * df[x_axis]
    resid = df[y_axis] - yhat0

    def _weights(shape): return np.random.choice([-1, 1], size=shape)
    
    D_bootstraps = []
    for b in range(n_boots):
        y_star = yhat0 + _weights(shape=n) * resid
        df_b = df.copy()
        df_b['y_star'] = y_star

        lin_b = smf.ols(f'y_star ~ {x_axis}', data=df_b).fit()
        loglik_lin_b = _calculate_gaussian_loglik(y_true=y_star, y_pred=lin_b.predict(df_b))

        X_b = df_b[[x_axis]].values
        spline_b = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X_b, y_star)
        loglik_spline_b = _calculate_gaussian_loglik(y_true=y_star, y_pred=spline_b.predict(X_b))

        D_b = 2 * (loglik_spline_b - loglik_lin_b)
        D_bootstraps.append(D_b)

    boot_res = pd.DataFrame({
        'D_bootstrap': D_bootstraps
    })
    boot_res['D_true'] = D_true
    boot_res['reject_linear'] = boot_res['D_bootstrap'] > D_true
    return boot_res

def _measure_distortion_rank_size_curve(df: pd.DataFrame, x_axis: str, y_axis: str):
    df = df[[x_axis, y_axis]].copy()
    lin = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    return np.max(np.abs(lin.resid))


def _get_data_for_country_year(country_year: Tuple[str, int], table_name: str, postgres: PostgresResource) -> pd.DataFrame:
    country, year = country_year
    q = f"""
    SELECT *
    FROM {table_name}
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    AND country = '{country}'
    AND year = {year}
    """
    return pd.read_sql(q, con=postgres.get_engine())


def _plot_non_linearity_figure_size_growth(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: List[str], title: str, df: pd.DataFrame, yaxlim: tuple = None):
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']
    inset_font_size = style_config['inset_font_size']
    inset_tick_font_size = style_config['inset_tick_font_size']

    x_axis = 'log_population'
    y_axis = 'log_growth'

    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    x_axis_inset = 'D_bootstrap'

    x_axis_inset_label = 'Test statistic ($D$)'
    y_axis_inset_label = 'Count'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    n_boots = 10 # TODO: change this back to 1000
    
    test_results = _test_linearity_of_size_growth_curve(df=df, x_axis=x_axis, y_axis=y_axis, n_boots=n_boots, lam=lam)

    x, y_spline, ci_low_spline, ci_high_spline = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)
    lin_model = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    y_lin = lin_model.predict(pd.DataFrame({x_axis: x}))

    ax.scatter(df[x_axis], df[y_axis], alpha=0.3, color='grey')
    ax.plot(x, y_lin, color=colors[0], linewidth=2,  label='Linear')
    ax.plot(x, y_spline, color=colors[1], linewidth=2, label='Spline')

    ax_inset = ax.inset_axes([0.65, 0.85, 0.3, 0.1])
    ax_inset.hist(test_results[x_axis_inset], bins=20, color='grey', alpha=0.5)
    ax_inset.axvline(test_results['D_true'].unique()[0], color='black', linewidth=2, linestyle='--')

    ax_inset.set_xlabel(x_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(y_axis_inset_label, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset.set_yticks([])
    sns.despine(ax=ax_inset)


    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)

    if yaxlim is not None:
        ax.set_ylim(yaxlim)

    sns.despine(ax=ax)
    ax.set_title(title, fontsize=title_font_size, fontfamily=font_family)
    return fig, ax

def _plot_non_linearity_figure_rank_size(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: List[str], title: str, plot_legend: bool, df: pd.DataFrame):
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']
    title_font_size = style_config['title_font_size']

    x_axis = 'log_rank'
    y_axis = 'log_population'

    x_axis_label = 'Rank (log)'
    y_axis_label = 'Size (log population)'

    lam = constants['PENALTY_RANK_SIZE_CURVE']

    x, y_spline, ci_low_spline, ci_high_spline = fit_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)
    lin_model = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    y_lin = lin_model.predict(pd.DataFrame({x_axis: x}))

    ax.scatter(df[x_axis], df[y_axis], alpha=0.3, color='grey')
    ax.plot(x, y_lin, color=colors[0], linewidth=2,  label='Linear')
    ax.plot(x, y_spline, color=colors[1], linewidth=2, label='Spline')

    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    sns.despine(ax=ax)
    if plot_legend:
        ax.legend(loc='upper right', frameon=False, fontsize=axis_font_size, ncol=1, bbox_to_anchor=(0.12, 1.2))
    ax.set_title(title, fontsize=title_font_size, fontfamily=font_family)
    return fig, ax


def _get_linear_vs_spline_slope(df: pd.DataFrame, x_axis: str, y_axis: str, lam: float) -> pd.DataFrame:
    lin = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    slope_lin = lin.params[x_axis]
    slope_spline = get_mean_derivative_penalized_b_spline(df=df, xaxis=x_axis, yaxis=y_axis, lam=lam)
    slopes = pd.DataFrame({
        'model': ['Linear', 'Spline'],
        'slope': [slope_lin, slope_spline]
    })
    return slopes

def _get_slopes_for_country_years(country_years: List[Tuple[str, int]], table_name: str, x_axis: str, y_axis: str, lam: float, postgres: PostgresResource) -> pd.DataFrame:
    q = f"""
    SELECT *
    FROM {table_name}
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    AND (country, year) IN {tuple(country_years)}
    """
    df = pd.read_sql(q, con=postgres.get_engine())
    slopes = df.groupby(['country', 'year']).apply(lambda x: _get_linear_vs_spline_slope(df=x, x_axis=x_axis, y_axis=y_axis, lam=lam))
    slopes = slopes.reset_index()
    slopes = slopes.rename(columns={0: 'slope'})
    return slopes

def _plot_barchart_slope_ols_vs_spline(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: List[str], x_axis_label: str, df: pd.DataFrame):
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    df = df.sort_values(by='country', ascending=False)
    sns.barplot(x='slope', y='country', hue='model', data=df, ax=ax, palette=colors, orient='h')
    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel('', fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.legend(loc='center', frameon=False, fontsize=axis_font_size, ncol=1, bbox_to_anchor=(0.5, 1.2), ncols=2)
    sns.despine(ax=ax)
    return fig, ax


def _plot_size_growth_linearity_test_results(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: List[str], df: pd.DataFrame):
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis_label = 'p-value threshold'
    y_axis_label = 'Share of samples with\np-value < threshold'
    
    pvalues = df.groupby(['country', 'year']).agg({'reject_linear': 'mean'}).reset_index().rename(columns={'reject_linear': 'pvalue'})
    pval = np.linspace(0.001, 0.1, 10)
    ys = []
    for p in pval:
        ys.append(pvalues[pvalues['pvalue'] < p].shape[0] / len(pvalues))


    ax.plot(pval, ys, color='black', linewidth=2)
    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_ylim(0, 0.4)
    sns.despine(ax=ax)
    return fig, ax


def _plot_rank_size_linearity_test_results(fig: plt.Figure, ax: plt.Axes, style_config: Dict[str, Any], colors: List[str], df: pd.DataFrame):
    font_family = style_config['font_family']
    axis_font_size = style_config['axis_font_size']
    tick_font_size = style_config['tick_font_size']

    x_axis_label = 'Max residual threshold'
    y_axis_label = 'Share of samples with\nmax residual > threshold'

    max_resid_thresholds = np.linspace(0.5, df['max_resid'].max(), 10)
    ys = []
    for mr in max_resid_thresholds:
        ys.append(df[df['max_resid'] > mr].shape[0] / len(df))


    ax.plot(max_resid_thresholds, ys, color='black', linewidth=2)
    ax.set_xlabel(x_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=axis_font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    ax.set_ylim(0, 0.3)
    sns.despine(ax=ax)
    return fig, ax
        
"""

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth()],
    group_name="supplementary_information",
    io_manager_key="postgres_io_manager"
)
def world_linearity_test_size_vs_growth(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Running linearity test for size vs growth")
    df = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    n_boots = 100
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    test_results = df.groupby(['country', 'year']).apply(lambda x: _test_linearity_of_size_growth_curve(df=x, x_axis='log_population', y_axis='log_growth', n_boots=n_boots, lam=lam))
    test_results = test_results.reset_index()
    test_results = test_results.drop(columns=['level_2'])
    test_results['analysis_id'] = MAIN_ANALYSIS_ID
    return test_results


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size()],
    group_name="supplementary_information",
    io_manager_key="postgres_io_manager"
)
def world_linearity_test_rank_size_curve(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Measuring distortion for rank size curve")
    df = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    test_results = df.groupby(['country', 'year']).apply(lambda x: _measure_distortion_rank_size_curve(df=x, x_axis='log_rank', y_axis='log_population'))
    test_results = test_results.reset_index()
    test_results = test_results.rename(columns={0: 'max_resid'})
    test_results['analysis_id'] = MAIN_ANALYSIS_ID
    return test_results


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size(), TableNamesResource().names.world.figures.world_size_vs_growth(), TableNamesResource().names.world.si.world_linearity_test_size_vs_growth(), TableNamesResource().names.world.si.world_linearity_test_rank_size_curve()],
    group_name="supplementary_information",
    io_manager_key="postgres_io_manager"
)
def figure_si_linear_rigidity(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting figure SI linear rigidity")
    figure_file_name = 'figure_si_linear_rigidity.png'
    figure_path = os.path.join(figure_si_dir, figure_file_name)

    fig = plt.figure(figsize=(15, 10))
    gs1 = gridspec.GridSpec(2, 3, wspace=0.3, hspace=0.3)
    ax1 = plt.subplot(gs1[0,0])
    ax2 = plt.subplot(gs1[0,1])
    gs2 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs1[0,2], height_ratios=[1, 2], wspace=0.3, hspace=0.4)
    ax3 = plt.subplot(gs2[0,0])
    ax4 = plt.subplot(gs2[1,0])

    ax5 = plt.subplot(gs1[1,0])
    ax6 = plt.subplot(gs1[1,1])
    gs3 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs1[1,2], height_ratios=[1, 2], wspace=0.3, hspace=0.4)
    ax7 = plt.subplot(gs3[0,0])
    ax8 = plt.subplot(gs3[1,0])


    size_growth_country_year_1 = ('BRA', 2010)
    size_growth_country_year_2 = ('IDN', 1975)
    rank_size_country_year_1 = ('THA', 1990)
    rank_size_country_year_2 = ('CHN', 1990)

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]


    idn_size_vs_growth = _get_data_for_country_year(country_year=size_growth_country_year_2, table_name=tables.names.world.figures.world_size_vs_growth(), postgres=postgres)
    _plot_non_linearity_figure_size_growth(fig=fig, ax=ax1, style_config=style_config, colors=colors, title='Indonesia', df=idn_size_vs_growth)

    bra_size_vs_growth = _get_data_for_country_year(country_year=size_growth_country_year_1, table_name=tables.names.world.figures.world_size_vs_growth(), postgres=postgres)
    _plot_non_linearity_figure_size_growth(fig=fig, ax=ax2, style_config=style_config, colors=colors, title='Brazil', df=bra_size_vs_growth, yaxlim=(-0.08, 0.2))

    size_growth_slopes_countries = _get_slopes_for_country_years(country_years=[size_growth_country_year_1, size_growth_country_year_2], table_name=tables.names.world.figures.world_size_vs_growth(), x_axis='log_population', y_axis='log_growth', lam=constants['PENALTY_SIZE_GROWTH_CURVE'], postgres=postgres)
    _plot_barchart_slope_ols_vs_spline(fig=fig, ax=ax3, style_config=style_config, colors=colors, x_axis_label='Size-growth slope', df=size_growth_slopes_countries)

    linearity_test_results = pd.read_sql(f"SELECT * FROM {tables.names.world.si.world_linearity_test_size_vs_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_size_growth_linearity_test_results(fig=fig, ax=ax4, style_config=style_config, colors=colors, df=linearity_test_results)


    thai_rank_size = _get_data_for_country_year(country_year=rank_size_country_year_1, table_name=tables.names.world.figures.world_rank_vs_size(), postgres=postgres)
    _plot_non_linearity_figure_rank_size(fig=fig, ax=ax5, style_config=style_config, colors=colors, title='Thailand', df=thai_rank_size, plot_legend=False)

    chn_rank_size = _get_data_for_country_year(country_year=rank_size_country_year_2, table_name=tables.names.world.figures.world_rank_vs_size(), postgres=postgres)
    _plot_non_linearity_figure_rank_size(fig=fig, ax=ax6, style_config=style_config, colors=colors, title='China', df=chn_rank_size, plot_legend=True)

    rank_size_slopes_countries = _get_slopes_for_country_years(country_years=[rank_size_country_year_1, rank_size_country_year_2], table_name=tables.names.world.figures.world_rank_vs_size(), x_axis='log_rank', y_axis='log_population', lam=constants['PENALTY_RANK_SIZE_CURVE'], postgres=postgres)
    rank_size_slopes_countries['slope'] = rank_size_slopes_countries['slope'].abs()
    _plot_barchart_slope_ols_vs_spline(fig=fig, ax=ax7, style_config=style_config, colors=colors, x_axis_label='Zipf exponent', df=rank_size_slopes_countries)

    linearity_test_results = pd.read_sql(f"SELECT * FROM {tables.names.world.si.world_linearity_test_rank_size_curve()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    _plot_rank_size_linearity_test_results(fig=fig, ax=ax8, style_config=style_config, colors=colors, df=linearity_test_results)

    fig.savefig(figure_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return materialize_image(path=figure_path)"""