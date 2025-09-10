# src/orchestration/defs/assets/figures/figure_utils.py
from pygam import LinearGAM, s
import numpy as np
import pandas as pd
from typing import Tuple, List
import matplotlib.pyplot as plt
import base64
import dagster as dg


def fit_penalized_b_spline(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    X = df[[xaxis]].values
    y = df[yaxis].values
    gam = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X, y)
    
    grid = gam.generate_X_grid(term=0)
    y_pdep, ci = gam.partial_dependence(term=0, X=grid, width=0.95)
    x = grid[:, 0]
    ci_low, ci_high = ci[:, 0], ci[:, 1]
    return x, y_pdep, ci_low, ci_high


def get_mean_derivative_penalized_b_spline(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float) -> float:
    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df, xaxis=xaxis, yaxis=yaxis, lam=lam)
    derivative = np.gradient(y, x)
    mean_derivative = np.mean(derivative)
    return mean_derivative


def get_bootstrap_ci_mean_derivative_penalized_b_spline(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    means = []
    for i in range(n_boots):
        df_bootstrap = df.sample(frac=1, replace=True)
        means.append(get_mean_derivative_penalized_b_spline(df=df_bootstrap, xaxis=xaxis, yaxis=yaxis, lam=lam))
    return np.median(means), np.quantile(means, 0.025), np.quantile(means, 0.975)


def cluster_bootstrap(data: pd.DataFrame, value_col: str, cluster_col: str, nboots: int) -> Tuple[float, float, float]:
    unique_clusters = data[cluster_col].unique()
    n_clusters = len(unique_clusters)
    bootstrap_means = []
    for i in range(nboots):
        sampled_cluster_ids = np.random.choice(unique_clusters, size=n_clusters, replace=True)
        bootstrap_sample = pd.concat([data[data[cluster_col] == c] for c in sampled_cluster_ids])
        bootstrap_means.append(np.mean(bootstrap_sample[value_col]))
    return np.median(bootstrap_means), np.percentile(bootstrap_means, 2.5), np.percentile(bootstrap_means, 97.5)


def materialize_image(path: str) -> dg.MaterializeResult:
    with open(path, "rb") as file:
        image_data = file.read()
    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"
    return dg.MaterializeResult(
        metadata={
            "preview": dg.MetadataValue.md(md_content)
        }
    )


def size_growth_slope_by_year_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    years = sorted(df['year'].unique().tolist())
    slopes_with_cis = []
    for y in years:
        df_y = df[df['year'] == y].copy()
        mean_value, ci_low, ci_high = get_bootstrap_ci_mean_derivative_penalized_b_spline(df=df_y, xaxis=xaxis, yaxis=yaxis, lam=lam, n_boots=n_boots)
        slopes_with_cis.append({
            'year': y,
            'size_growth_slope': mean_value,
            'ci_low': ci_low,
            'ci_high': ci_high
        })

    slopes_with_cis = pd.DataFrame(slopes_with_cis)
    return slopes_with_cis
    

def rank_size_slope_by_year_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    years = sorted(df['year'].unique().tolist())
    slopes_with_cis = []
    for y in years:
        df_y = df[df['year'] == y].copy()
        mean_value, ci_low, ci_high = get_bootstrap_ci_mean_derivative_penalized_b_spline(df=df_y, xaxis=xaxis, yaxis=yaxis, lam=lam, n_boots=n_boots)
        slopes_with_cis.append({
            'year': y,
            'rank_size_slope': -1 * mean_value,
            'ci_low': -1 * ci_low,
            'ci_high': -1 * ci_high
        })

    slopes_with_cis = pd.DataFrame(slopes_with_cis)
    return slopes_with_cis


def annotate_letter_label(axes: List[plt.Axes], left_side: List[bool], letter_label_font_size: int, font_family: str) -> None:
    for i, ax in enumerate(axes):
        y = 0.99
        x = 0.05 if left_side[i] else 0.95
        ax.annotate(
            text=f'{chr(65 + i)}',
            xy=(x, y),
            xycoords='axes fraction',
            ha='left',
            va='top',
            fontsize=letter_label_font_size,
            fontfamily=font_family
        )
