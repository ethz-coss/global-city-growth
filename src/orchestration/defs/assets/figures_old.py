import dagster as dg
import pandas as pd
import numpy as np
import base64
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
import plotly.express as px
import seaborn as sns
import geopandas as gpd
import os

import matplotlib.colors as mcolors
from matplotlib.colors import to_rgba
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.gridspec as gridspec

from ..resources.resources import PostgresResource, TableNamesResource


@dg.asset(
    deps=["usa_size_vs_growth"],
    group_name="figures"
)
def figure_3_size_growth_curve(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:

    context.log.info(f"Creating figure 3: size vs growth curve")
    
    sql_query = f"""
    SELECT  year, 
            log_growth, 
            log_population_y1,
            CASE WHEN year < 1900 THEN '1850-1880'
                 WHEN year < 1950 THEN '1900-1940'
                 ELSE '1990-2020'
            END AS epoch
    FROM {tables.names.usa.final.usa_size_vs_growth()}
    """
    size_vs_growth_by_epoch = pd.read_sql(sql_query, postgres.get_engine())

    fig, ax = plt.subplots(figsize=(5, 5))

    font_size = 14
    font_family = 'Helvetica Neue'
    tick_size = 12
    colors = [px.colors.qualitative.Plotly[7], px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    legend_size = 12
    legend_loc = 'upper left'

    x_axis = 'log_population_y1'
    y_axis = 'log_growth'
    x_axis_label = 'Size (log population)'
    y_axis_label = 'Growth rate (log)'

    spline_degree = 5
    spline_df = 5
    spline_term = f"bs({x_axis}, df={spline_df}, degree={spline_degree}, include_intercept=False)"

    save_path = '/app/figures/usa_growth_vs_size_by_epoch.png'

    shift = 0.1


    epochs = sorted(size_vs_growth_by_epoch['epoch'].unique().tolist())
    for i, e in enumerate(epochs):
        df_e = size_vs_growth_by_epoch[size_vs_growth_by_epoch['epoch'] == e]
        spline_model = smf.ols(f'{y_axis} ~ {x_axis} + {spline_term}', data=df_e).fit()

        x_min, x_max = df_e[x_axis].min() + shift, df_e[x_axis].max() - shift
        x_ = pd.DataFrame({x_axis: np.linspace(x_min, x_max, 100)})
        y_spline = spline_model.get_prediction(x_).summary_frame(alpha=0.05) 

        plot_df = pd.concat([x_, y_spline], axis=1)
        ax.plot(plot_df[x_axis], plot_df['mean'], color=colors[i], label=e)
        ax.fill_between(plot_df[x_axis], plot_df['mean_ci_lower'], plot_df['mean_ci_upper'], color=colors[i], alpha=0.2)


    ax.set_xlabel(x_axis_label, fontsize=font_size, fontfamily=font_family)
    ax.set_ylabel(y_axis_label, fontsize=font_size, fontfamily=font_family)
    ax.tick_params(axis='both', which='major', labelsize=tick_size)
    ax.legend(fontsize=legend_size, loc=legend_loc, frameon=False)
    sns.despine(ax=ax)
    fig.tight_layout()
    fig.savefig(save_path, dpi=300)
    plt.close(fig)


    with open(save_path, "rb") as file:
        image_data = file.read()

     # Convert the image data to base64
    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"

    return dg.MaterializeResult(
        metadata={
            "preview": dg.MetadataValue.md(md_content)
        }
    )
    


