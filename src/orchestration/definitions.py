from dagster import Definitions, in_process_executor

from .defs.assets.ipums_full_count import ipums_full_count_table_raw, crosswalk_hist_id_to_hist_census_place_table_raw, ipums_full_count_table_clean, crosswalk_hist_id_to_hist_census_place_table_clean, ipums_full_count_census_with_census_place_id, ipums_full_count_census_with_census_place_id_all_years, census_place_population, ipums_full_count_individual_migration, census_place_migration
from .defs.assets.source_assets import usa_hist_census_place_population, usa_hist_census_place_migration, usa_nhgis_census_place_population_raw_1990_2020, usa_nhgis_census_place_geom_raw, usa_hist_census_place_geom_raw, usa_states_geom_raw
from .defs.resources.resources import duckdb_resource, storage_resource, postgres_resource


defs = Definitions(
    assets=[
        # IPUMS Full Count
        ipums_full_count_table_raw,
        crosswalk_hist_id_to_hist_census_place_table_raw,
        ipums_full_count_table_clean,
        crosswalk_hist_id_to_hist_census_place_table_clean,
        ipums_full_count_census_with_census_place_id,
        ipums_full_count_census_with_census_place_id_all_years,
        ipums_full_count_individual_migration,
        census_place_population,
        census_place_migration,

        # USA Bronze
        usa_hist_census_place_population,
        usa_hist_census_place_migration,
        usa_nhgis_census_place_population_raw_1990_2020,
        usa_nhgis_census_place_geom_raw,
        usa_hist_census_place_geom_raw,
        usa_states_geom_raw
    ],
    resources={
        "duckdb": duckdb_resource,
        "storage": storage_resource,
        "postgres": postgres_resource
    },
    executor=in_process_executor
)