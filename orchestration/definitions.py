from dagster import Definitions, in_process_executor

from .defs.assets import ipums_full_count_table_raw, crosswalk_hist_id_to_hist_census_place_table_raw, ipums_full_count_table_clean, crosswalk_hist_id_to_hist_census_place_table_clean, ipums_full_count_census_with_census_place_id, ipums_full_count_census_with_census_place_id_all_years, census_place_population, ipums_full_count_individual_migration, census_place_migration
from .defs.resources import duckdb_resource, storage_resource


defs = Definitions(
    assets=[
        ipums_full_count_table_raw,
        crosswalk_hist_id_to_hist_census_place_table_raw,
        ipums_full_count_table_clean,
        crosswalk_hist_id_to_hist_census_place_table_clean,
        ipums_full_count_census_with_census_place_id,
        ipums_full_count_census_with_census_place_id_all_years,
        ipums_full_count_individual_migration,
        census_place_population,
        census_place_migration
    ],
    resources={
        "duckdb": duckdb_resource,
        "storage": storage_resource,
    },
    executor=in_process_executor
)