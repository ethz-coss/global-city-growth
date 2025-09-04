from pygam import LinearGAM, s
import numpy as np
import pandas as pd
from typing import Tuple
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


def cluster_bootstrap(data: pd.DataFrame, value_col: str, cluster_col: str, nboots: int) -> Tuple[float, float, float]:
    unique_clusters = data[cluster_col].unique()
    n_clusters = len(unique_clusters)
    bootstrap_means = []
    for i in range(nboots):
        sampled_cluster_ids = np.random.choice(unique_clusters, size=n_clusters, replace=True)
        bootstrap_sample = pd.concat([data[data[cluster_col] == c] for c in sampled_cluster_ids])
        bootstrap_means.append(np.mean(bootstrap_sample[value_col]))
    return np.mean(bootstrap_means), np.percentile(bootstrap_means, 2.5), np.percentile(bootstrap_means, 97.5)


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