from dagster import Definitions, in_process_executor

from .defs.assets import raw_ipums_full_count_table, raw_crosswalk_hist_id_to_hist_census_place_table
from .defs.resources import duckdb_resource, storage_resource


defs = Definitions(
    assets=[
        raw_ipums_full_count_table,
        raw_crosswalk_hist_id_to_hist_census_place_table
    ],
    resources={
        "duckdb": duckdb_resource,
        "storage": storage_resource,
    },
    executor=in_process_executor
)