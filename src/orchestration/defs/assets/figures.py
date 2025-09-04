import dagster as dg
import pandas as pd
import numpy as np
import base64
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
import plotly.express as px
import seaborn as sns
import matplotlib.colors as mcolors

from ..resources.resources import PostgresResource, TableNamesResource

## Figure 2

def _create_bicolor_cmap(cmap_neg, cmap_pos, midpoint_frac, name='bicolor_cmap', N=256):
    """
    Combines two colormaps into a single new one.

    Args:
        cmap_neg (str): Name of the colormap for the range below the midpoint.
        cmap_pos (str): Name of the colormap for the range above the midpoint.
        midpoint_frac (float): The center point as a fraction (0 to 1).
        name (str): The name for the new colormap.
        N (int): The number of colors in the new colormap.

    Returns:
        matplotlib.colors.LinearSegmentedColormap: The new combined colormap.
    """
    # Get the colormap objects
    cmap_neg_obj = plt.get_cmap(cmap_neg)
    cmap_pos_obj = plt.get_cmap(cmap_pos)
    print(midpoint_frac)

    # Determine the number of colors for each part
    n_neg = int(np.round(N * midpoint_frac))
    n_pos = N - n_neg

    # Sample colors from each colormap
    neg_colors = cmap_neg_obj(np.linspace(1 - midpoint_frac, 1, n_neg))
    pos_colors = cmap_pos_obj(np.linspace(0.0, 1 - midpoint_frac, n_pos))
    
    # Combine the color lists
    all_colors = np.vstack((neg_colors, pos_colors))

    # Create the new colormap
    return mcolors.LinearSegmentedColormap.from_list(name, all_colors)


def _plot_world_map_with_colorbar(fig, ax, style_config, gdf):
    # Parameters
    col = 'growth_exponent'
    midpoint = 0
    
    missing_hatch = '///'
    missing_color = 'white'
    projection_epsg = 4087

    font_family = style_config['font_family']
    inset_font_size = style_config['inset_font_size'] 
    inset_tick_font_size = style_config['inset_tick_font_size']
    inset_text_label_font_size = style_config['inset_text_label_font_size']
    colorbar_label_fontsize = style_config['colorbar_label_font_size']

    colorbar_pos = [0.4, 0.13, 0.2, 0.025]
    colorbar_label_pos = 'top'
    colorbar_label = 'Size-growth slope'

    inset_distribution_pos = [0.66, 0.2, 0.1, 0.2]
    inset_distribution_ylabel = 'Density'
    inset_distribution_xlabel = 'Size-growth slope'

    inset_growth_size_pos = [0.1, 0.2, 0.15, 0.2]
    inset_growth_size_xlabel = 'Size (log population)'
    inset_growth_size_ylabel = 'Growth rate (log)'

    # Map 
    
    gdf_proj = gdf.to_crs(epsg=projection_epsg)
    vmin, vmax = np.nanmin(gdf_proj[col]), np.nanmax(gdf_proj[col])
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    midpoint_frac = (midpoint - vmin) / (vmax - vmin)
    rescaled_cmap = create_bicolor_cmap(cmap_neg='Greys_r', cmap_pos='Blues', midpoint_frac=midpoint_frac)


    ax.set_axis_off()
    divider = make_axes_locatable(ax)
    cax = fig.add_axes(colorbar_pos)

    gdf_proj.plot(column=col, ax=ax, legend=True, cax=cax, cmap=rescaled_cmap, norm=norm, linewidth=0.5, edgecolor='0.5', missing_kwds={
                        "color": missing_color,
                        "edgecolor": to_rgba('#D3D3D3', 0.8),
                        "hatch": missing_hatch,
                    }, legend_kwds={
                        'orientation': "horizontal",
                    })


    for spine in cax.spines.values(): spine.set_visible(False)
    cax.set_xlabel(colorbar_label, fontsize=colorbar_label_fontsize, fontfamily=font_family)
    cax.xaxis.set_label_position(colorbar_label_pos)


    # Inset distribution

    ax_inset = fig.add_axes(inset_distribution_pos)
    sns.kdeplot(gdf_proj[col], ax=ax_inset, color='black', fill=True, alpha=0.2)
    ax_inset.axvline(x=gdf_proj[col].median(), color='black', linestyle='--', linewidth=0.5)
    ax_inset.text(gdf_proj[col].median() + 0.01, 21, f'Median: {gdf_proj[col].median():.2f}', fontsize=inset_text_label_font_size, ha='center', va='center', fontfamily=font_family)
    ax_inset.set_xlabel(inset_distribution_xlabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.set_ylabel(inset_distribution_ylabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset.set_title('')
    ax_inset.set_yticks([])
    sns.despine(ax=ax_inset)

    # Inset growth size

    ax_inset2 = fig.add_axes(inset_growth_size_pos)

    x_ = [4, 5, 6, 7]
    y_ = [0.04, 0.045, 0.07, 0.06]
    p = np.polyfit(x_, y_, 4)
    x_ = np.linspace(4, 7, 100)
    y_ = np.polyval(p, x_)
    ax_inset2.plot(x_, y_, color='black', linewidth=1)

    p_der = np.polyder(p)
    x_points = [4.5, 6.5]
    shift_y = [-0.008, 0.008]
    color_tangent = px.colors.qualitative.Plotly[1]
    for i, x_tan in enumerate(x_points):
        y_tan = np.polyval(p, x_tan)
        slope = np.polyval(p_der, x_tan)
        x_line = np.linspace(x_tan - 0.5, x_tan + 0.5, 2)
        y_line = slope * (x_line - x_tan) + y_tan
        ax_inset2.plot(x_line, y_line, color=color_tangent, linestyle='--', linewidth=1.5)
        shift_y_i = shift_y[i]
        ax_inset2.text(x_tan, y_tan + shift_y_i, f'Slope', fontsize=inset_text_label_font_size, ha='center', va='center', fontfamily=font_family, color=color_tangent)


    ax_inset2.annotate(
        text=r'Size-growth slope' + '=\naverage slope of this curve',
        xy=(5.5, 0.06),  # The point on the curve to point to
        xytext=(5.5, 0.12),    # Where the text is located
        xycoords='data',
        arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
        ha='center',
        va='center'
    )
    # ax_inset2.text(5.5, 0.12, f'Size-growth slope =\naverage slope of this curve', fontsize=inset_font_size, fontfamily=font_family, ha='center', va='center')
    ax_inset2.set_xlabel(inset_growth_size_xlabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset2.set_ylabel(inset_growth_size_ylabel, fontsize=inset_font_size, fontfamily=font_family)
    ax_inset2.tick_params(axis='both', which='major', labelsize=inset_tick_font_size)
    ax_inset2.set_yticks([])
    ax_inset2.set_xlim(4, 7)
    ax_inset2.set_ylim(0.02, 0.09)
    sns.despine(ax=ax_inset2)
    return fig, ax

@dg.asset(
    deps=["world_size_growth_slopes"],
    group_name="figures"
)
def figure_2_map(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    pass


@dg.asset(
    deps=["usa_rank_size_slopes"],
    group_name="figures"
)
def figure_2_plots(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    pass
    

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
    


