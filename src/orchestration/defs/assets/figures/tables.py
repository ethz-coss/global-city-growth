import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import statsmodels.formula.api as smf
import plotly.express as px
import seaborn as sns
import os
from typing import Tuple, Dict
from stargazer.stargazer import Stargazer
import re


from ...resources.resources import PostgresResource, TableNamesResource
from .figure_config import style_config, figure_dir, table_dir, region_colors, MAIN_ANALYSIS_ID
from .figure_utils import fit_penalized_b_spline, materialize_image, get_bootstrap_ci_mean_derivative_penalized_b_spline
from ..constants import constants


def _make_table_slopes_by_urbanization(df: pd.DataFrame, x_axis: str, y_axis: str) -> pd.DataFrame:
    ols_reg_no_fe = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    ols_reg_with_country_year_fe = smf.ols(f'{y_axis} ~ {x_axis} + C(country) + C(year)', data=df).fit()

    results = []

    for model in [ols_reg_no_fe, ols_reg_with_country_year_fe]:
        coef = model.params.get(x_axis)
        bse = model.bse.get(x_axis)
        pval = model.pvalues.get(x_axis)
        r2 = model.rsquared
        nobs = model.nobs
        results.append([coef, bse, pval, r2, nobs])

    # Mapping model types for the final table
    year_fe = ['no', 'yes']
    country_fe = ['no', 'yes']

    table = pd.DataFrame(results, columns=['coef', 'bse', 'pval', 'r2', 'nobs'])
    table['year_fe'] = year_fe
    table['country_fe'] = country_fe
    return table

def _format_table_slopes_by_urbanization_for_latex(table: pd.DataFrame, x_label: str) -> pd.DataFrame:
    m = len(table)  

    def stars(p): return '***' if p<0.01 else '**' if p<0.05 else '*' if p<0.10 else ''
    coef_str = [f"{table.loc[i,'coef']:.3f}{stars(table.loc[i,'pval'])}" for i in range(m)]
    bse_str   = [f"({table.loc[i,'bse']:.3f})" for i in range(m)]
    n_str    = [f"{int(table.loc[i,'nobs'])}" for i in range(m)]
    r2_str   = [f"{table.loc[i,'r2']:.3f}" for i in range(m)]
    cfe_str  = [str(table.loc[i,'country_fe']).capitalize() for i in range(m)]
    yfe_str  = [str(table.loc[i,'year_fe']).capitalize() for i in range(m)]

    out = pd.DataFrame([
        [x_label,              *coef_str],
        ["",                   *bse_str],
        ["Country fixed effect", *cfe_str],
        ["Year fixed effect",    *yfe_str],
        ["Observations",         *n_str],
        [r"$R^2$",               *r2_str],
    ], columns=[''] + [f"col{i}" for i in range(1, m+1)]) 

    return out

def _get_latex_from_formatted_table_slopes_by_urbanization(table: pd.DataFrame, y_label_1: str, y_label_2: str) -> str:
    m = table.shape[1]
    latex = table.to_latex(
        index=False,
        header=False,                    
        escape=False,
        column_format='l' + 'c' * m,
    )

    latex = latex.replace("\r\n", "\n").replace("\r", "\n")

    header_line = (
        r"\\[-1.8ex]\hline"
        r"\hline \\[-1.8ex]"
        f"Independent \\textbackslash \ Dependent& \multicolumn{{{m // 2}}}{{c}}{{{y_label_1}}} & \multicolumn{{{m // 2}}}{{c}}{{{y_label_2}}} \\\\\n"
    )   

    footer_line = (
        r"\hline"
        r"\hline \\[-1.8ex]"
        f"& \multicolumn{{{m - 1}}}{{r}}{{$^{{*}}$p$<$0.1; $^{{**}}$p$<$0.05; $^{{***}}$p$<$0.01}} \\\\\n"
    )

    latex = latex.replace("\\toprule\n", header_line, 1)
    latex = latex.replace("\\bottomrule\n", footer_line, 1)
    return latex


def _make_table_2(df_size_growth_slopes: pd.DataFrame, df_rank_size_slopes: pd.DataFrame, context) -> str:
    x_axis_1 = 'urban_population_share'
    y_axis_1 = 'size_growth_slope'
    x_label_1 = 'Urban population share'
    y_label_1 = 'Size-growth slope'
   
   
    x_axis_2 = 'urban_population_share'
    y_axis_2 = 'rank_size_slope'
    x_label_2 = 'Urban population share'
    y_label_2 = 'Zipf exponent'

    table_1 = _make_table_slopes_by_urbanization(df=df_size_growth_slopes, x_axis=x_axis_1, y_axis=y_axis_1)
    table_2 = _make_table_slopes_by_urbanization(df=df_rank_size_slopes, x_axis=x_axis_2, y_axis=y_axis_2)

    table_1_formatted = _format_table_slopes_by_urbanization_for_latex(table=table_1, x_label=x_label_1)
    table_2_formatted = _format_table_slopes_by_urbanization_for_latex(table=table_2, x_label=x_label_2)

    table_formatted = pd.concat([table_1_formatted, table_2_formatted[[c for c in table_2_formatted.columns if c.startswith("col")]]], axis=1)
    latex = _get_latex_from_formatted_table_slopes_by_urbanization(table=table_formatted, y_label_1=y_label_1, y_label_2=y_label_2)
    return latex

def _get_latex_from_formatted_table_dataset_summary(table: pd.DataFrame) -> str:
    latex = table.to_latex(
        index=False,
        escape=False,
        column_format='l' + 'c' * table.shape[1]
    )
    latex = latex.replace("\r\n", "\n").replace("\r", "\n")
    header_line = (
        r"\\[-1.8ex]\hline"
        r"\hline \\[-1.8ex]"
    )   

    footer_line = (
        r"\hline"
        r"\hline \\[-1.8ex]"
    )

    latex = latex.replace("\\toprule\n", header_line, 1)
    latex = latex.replace("\\bottomrule\n", footer_line, 1)
    return latex

def _make_table_dataset_summary(df_usa_dataset_summary_table: pd.DataFrame, df_world_dataset_summary_table: pd.DataFrame, context) -> str:
    table = pd.concat([df_world_dataset_summary_table, df_usa_dataset_summary_table], axis=0)
    table = table.drop(columns=['analysis_id'])
    table['Dataset'] = ['Global cities', 'USA cities']
    table = table.rename(columns={'num_cities': 'Cities', 'num_countries': 'Countries', 'num_years': 'Years', 'min_year': 'Min year', 'max_year': 'Max year'})
    col_order = ['Dataset'] + [c for c in table.columns if c != 'Dataset']
    table = table[col_order]
    latex = _get_latex_from_formatted_table_dataset_summary(table=table)
    return latex

@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_dataset_summary_table(), TableNamesResource().names.world.figures.world_dataset_summary_table()],
    group_name="figures"
)
def table_1(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    table_file_name = 'table_1.txt'
    table_path = os.path.join(table_dir, table_file_name)

    usa_dataset_summary_table = pd.read_sql(f"SELECT * FROM {tables.names.usa.figures.usa_dataset_summary_table()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    world_dataset_summary_table = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_dataset_summary_table()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    latex_table = _make_table_dataset_summary(df_usa_dataset_summary_table=usa_dataset_summary_table, df_world_dataset_summary_table=world_dataset_summary_table, context=context)

    with open(table_path, 'w') as f:
        f.write(latex_table)

    return dg.MaterializeResult(
        metadata={
            "path": dg.MetadataValue.path(table_path),
            "num_records_processed": len(usa_dataset_summary_table),
        }
    )



@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_urbanization(), TableNamesResource().names.world.figures.world_rank_size_slopes_urbanization()],
    group_name="figures"
)
def table_2(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating table 2")
    table_file_name = 'table_2.txt'
    table_path = os.path.join(table_dir, table_file_name)

    world_size_growth_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_growth_slopes_urbanization()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    world_rank_size_slopes_urbanization = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_size_slopes_urbanization()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())  
    latex_table = _make_table_2(df_size_growth_slopes=world_size_growth_slopes_urbanization, df_rank_size_slopes=world_rank_size_slopes_urbanization, context=context)

    with open(table_path, 'w') as f:
        f.write(latex_table)


    return dg.MaterializeResult(
        metadata={
            "path": dg.MetadataValue.path(table_path),
            "num_records_processed": len(world_size_growth_slopes_urbanization),
        }
    )