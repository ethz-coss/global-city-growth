from dagster_duckdb import DuckDBResource
import dagster as dg

from .resources import StorageResource
from configs.constants import IPUMS_HISTORICAL_YEARS, IPUMS_NHGIS_POP_YEARS, GHSL_RASTER_YEARS

ipums_historical_years_partitions = dg.StaticPartitionsDefinition([str(y) for y in IPUMS_HISTORICAL_YEARS])
# Define the demographic data asset
@dg.asset(
    partitions_def=ipums_historical_years_partitions,
    group_name="bronze"
)
def raw_ipums_full_count_table(context: dg.AssetExecutionContext, duckdb: DuckDBResource, storage: StorageResource):
    """An asset for the demographic data table for a given year."""
    year = context.partition_key
    table_name = f'ipums_full_count_{year}'
    file_path = storage.paths.usa.ipums_full_count.ipums_full_count(year)

    context.log.info(f"Loading IPUMS full count data for {year} into table {table_name}")

    sql_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} AS
    SELECT * FROM read_csv('{file_path}');
    """

    with duckdb.get_connection() as con:
        con.sql(sql_query)
        num_rows = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    
    context.add_output_metadata({"num_rows": num_rows, "table": table_name})

# Define the geographic data asset
@dg.asset(
    partitions_def=ipums_historical_years_partitions,
    group_name="bronze"
)
def raw_crosswalk_hist_id_to_hist_census_place_table(context: dg.AssetExecutionContext, duckdb: DuckDBResource, storage: StorageResource):
    """An asset for the geographic data table for a given year."""
    year = context.partition_key
    table_name = f"crosswalk_hist_id_to_hist_census_place_{year}"
    file_path = storage.paths.usa.census_place_project.crosswalk_ipums_full_count_hist_id_to_hist_census_place(year)

    context.log.info(f"Loading crosswalk data for {year} into table {table_name}")

    sql_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} AS
    SELECT * FROM read_csv('{file_path}');
    """

    with duckdb.get_connection() as con:
        con.sql(sql_query)
        num_rows = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    context.add_output_metadata({"num_rows": num_rows, "table": table_name})







