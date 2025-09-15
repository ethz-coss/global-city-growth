from dagster import Definitions, in_process_executor

from .defs.assets.ipums_full_count import ipums_full_count_table_raw, crosswalk_hist_id_to_hist_census_place_table_raw, ipums_full_count_table_clean, crosswalk_hist_id_to_hist_census_place_table_clean, ipums_full_count_census_with_census_place_id, ipums_full_count_census_with_census_place_id_all_years, census_place_population, ipums_full_count_individual_migration, census_place_migration
from .defs.assets.usa_sources import usa_hist_census_place_population, usa_hist_census_place_migration, usa_nhgis_census_place_population_1990_2020_raw, usa_nhgis_census_place_geom_all_years_raw, usa_hist_census_place_geom_raw, usa_states_geom_raw
from .defs.assets.usa import usa_crosswalk_nhgis_census_place_to_connected_component, usa_raster_census_place_convolved_year, usa_raster_census_place_convolved_all_years, usa_crosswalk_component_id_to_cluster_id
from .defs.assets.world_sources import world_raster_ghsl_pop, world_raster_ghsl_smod, world_country_borders_raw, world_urbanization_raw, world_crosswalk_cshapes_code_to_iso_code, world_raster_ghsl_pop_all_years, world_raster_ghsl_smod_all_years, world_country_region
from .defs.assets.world import world_crosswalk_component_id_to_cluster_id
from .defs.assets.figures.figure_data_prep import analysis_parameters, world_size_growth_slopes, world_rank_size_slopes, world_region_regression_with_urbanization_controls
from .defs.assets.figures.figure_2 import figure_2_map, figure_2_plots
from .defs.assets.figures.figure_3 import figure_3
from .defs.assets.figures.figure_4 import figure_4
from .defs.assets.figures.tables import table_1, table_2
from .defs.assets.dbt import dbt_warehouse, my_example_asset, incremental_example_table
from .defs.assets.figures.si.si_linear_rigidity import world_linearity_test_size_vs_growth, world_linearity_test_rank_size_curve, figure_si_linear_rigidity
from .defs.assets.figures.si.si_robustness import world_robustness_tables, usa_robustness_figure
from .defs.resources.resources import duckdb_resource, storage_resource, postgres_resource, dbt_resource, table_names_resource, pipes_subprocess_resource, postgres_pandas_io_manager


defs = Definitions(
    assets=[
        # USA
        ## IPUMS Full Count
        ipums_full_count_table_raw,
        crosswalk_hist_id_to_hist_census_place_table_raw,
        ipums_full_count_table_clean,
        crosswalk_hist_id_to_hist_census_place_table_clean,
        ipums_full_count_census_with_census_place_id,
        ipums_full_count_census_with_census_place_id_all_years,
        ipums_full_count_individual_migration,
        census_place_population,
        census_place_migration,

        ## USA Raw
        usa_hist_census_place_population,
        usa_hist_census_place_migration,
        usa_nhgis_census_place_population_1990_2020_raw,
        usa_nhgis_census_place_geom_all_years_raw,
        usa_hist_census_place_geom_raw,
        usa_states_geom_raw,

        ## USA Intermediate
        usa_crosswalk_nhgis_census_place_to_connected_component,
        usa_raster_census_place_convolved_year,
        usa_raster_census_place_convolved_all_years,
        usa_crosswalk_component_id_to_cluster_id,
        
        # World
        ## World Raw
        world_raster_ghsl_pop,
        world_raster_ghsl_smod,
        world_raster_ghsl_pop_all_years,
        world_raster_ghsl_smod_all_years,
        world_country_borders_raw,
        world_urbanization_raw,
        world_crosswalk_cshapes_code_to_iso_code,
        world_country_region,

        ## World Intermediate
        world_crosswalk_component_id_to_cluster_id,
        
        # Shared
        ## DBT
        dbt_warehouse,
        my_example_asset,
        incremental_example_table,

        ## Figure Data Prep
        analysis_parameters,
        world_size_growth_slopes,
        world_rank_size_slopes,
        world_region_regression_with_urbanization_controls,

        ## Figures
        figure_2_map,
        figure_2_plots,
        figure_3,
        figure_4,

        ## Tables
        table_1,
        table_2,

        ## Supplementary Information
        world_linearity_test_size_vs_growth,
        world_linearity_test_rank_size_curve,
        figure_si_linear_rigidity,
        world_robustness_tables,
        usa_robustness_figure,
    ],
    resources={
        "duckdb": duckdb_resource,
        "storage": storage_resource,
        "postgres": postgres_resource,
        "dbt": dbt_resource,
        "tables": table_names_resource,
        "bash": pipes_subprocess_resource,
        "postgres_io_manager": postgres_pandas_io_manager
    },
    executor=in_process_executor
)