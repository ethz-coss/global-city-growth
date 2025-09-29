from typing import Tuple, Callable, Optional
import pandas as pd
import numpy as np
from pygam import LinearGAM, s
import statsmodels.formula.api as smf


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

def get_ols_slope(df: pd.DataFrame, xaxis: str, yaxis: str) -> float:
    ols = smf.ols(f'{yaxis} ~ {xaxis}', data=df).fit()
    return ols.params[xaxis]

def bootstrap_ci(estimator: Callable[[pd.DataFrame], float], df: pd.DataFrame, nboots: int = 1000, alpha: float = 0.05) -> Tuple[float, float, float]:
    stats = np.empty(nboots, dtype=float)
    n = len(df)
    for b in range(nboots):
        idx = np.random.randint(0, n, n)
        stats[b] = float(estimator(df.iloc[idx]))

    return np.median(stats), np.quantile(stats, alpha / 2), np.quantile(stats, 1 - alpha / 2)


def clustered_boostrap_ci(estimator: Callable[[pd.DataFrame], float], df: pd.DataFrame, cluster_col: str, nboots: int = 1000, alpha: float = 0.05) -> Tuple[float, float, float]:
    clusters = df[cluster_col].dropna().unique()
    k = len(clusters)
    stats = np.empty(nboots, dtype=float)
    for b in range(nboots):
        sampled_ids = np.random.choice(clusters, size=k, replace=True)
        sample = pd.concat([df[df[cluster_col] == cid] for cid in sampled_ids], ignore_index=True)
        stats[b] = float(estimator(sample))

    return np.median(stats), np.quantile(stats, alpha / 2), np.quantile(stats, 1 - alpha / 2)


def size_growth_slope_by_year_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    slopes = _slopes_with_cis(df=df, xaxis=xaxis, yaxis=yaxis, lam=lam, n_boots=n_boots, col_name='size_growth_slope', sign_value=1)
    return slopes
    

def rank_size_slope_by_year_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int) -> Tuple[float, float, float]:
    slopes = _slopes_with_cis(df=df, xaxis=xaxis, yaxis=yaxis, lam=lam, n_boots=n_boots, col_name='rank_size_slope', sign_value=-1)
    return slopes


def _slopes_with_cis(df: pd.DataFrame, xaxis: str, yaxis: str, lam: float, n_boots: int, col_name: str, sign_value: int) -> pd.DataFrame:
    years = sorted(df['year'].unique().tolist())
    slopes_with_cis = []
    for y in years:
        df_y = df[df['year'] == y].copy()
        estimator = lambda x: get_mean_derivative_penalized_b_spline(x, xaxis=xaxis, yaxis=yaxis, lam=lam)
        mean_value, ci_low, ci_high = bootstrap_ci(estimator=estimator, df=df_y, nboots=n_boots)
        slopes_with_cis.append({
            'year': y,
            col_name: sign_value * mean_value,
            'ci_low': sign_value * ci_low,
            'ci_high': sign_value * ci_high
        })

    slopes_with_cis = pd.DataFrame(slopes_with_cis)
    return slopes_with_cis