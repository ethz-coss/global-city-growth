import dagster as dg
from dagster_duckdb import DuckDBResource
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import text
import geopandas as gpd

from ..resources.resources import PostgresResource, StorageResource, TableNamesResource
from .ipums_full_count import census_place_migration as census_place_migration_duckdb, census_place_population as census_place_population_duckdb
from .download import nhgis_place_population_1990_2020_downloaded, raw_data_zenodo, nhgis_place_geom_1900_2010_downloaded



def _copy_table_from_duckdb_to_postgres(table_duckdb: str, table_postgres: str, duckdb: DuckDBResource, postgres: PostgresResource):
    # Drop table for idempotency
    engine = postgres.get_engine()
    with engine.begin() as conn:
        query = f'DROP TABLE IF EXISTS {table_postgres}'
        conn.execute(text(query))

    # Copy table from duckdb to postgres
    with duckdb.get_connection() as con:
        con.sql(f"""
        ATTACH '{postgres.connection_string}' AS postgres_db (TYPE POSTGRES);
        CREATE TABLE postgres_db.{table_postgres} AS SELECT * FROM {table_duckdb};
        DETACH postgres_db;
        """)

@dg.asset(
    deps=[census_place_population_duckdb],
    kinds={'postgres'},
    group_name="usa_raw",
    pool="duckdb_write"
)
def usa_hist_census_place_population(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Copying census place population from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=tables.names.usa.ipums_full_count.census_place_population(),
        table_postgres=tables.names.usa.sources.usa_hist_census_place_population(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    deps=[census_place_migration_duckdb],
    kinds={'postgres'},
    group_name="usa_raw",
    pool="duckdb_write"
)
def usa_hist_census_place_migration(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Copying census place migration from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=tables.names.usa.ipums_full_count.census_place_migration(),
        table_postgres=tables.names.usa.sources.usa_hist_census_place_migration(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    deps=[nhgis_place_population_1990_2020_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    io_manager_key="postgres_io_manager"
)
def usa_nhgis_census_place_population_1990_2020_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    context.log.info(f"Copying census place population from nhgis to postgres")
    data = pd.read_csv(storage.paths.usa.nhgis.census_place_pop_1990_2020(), encoding='latin1')
    return data

@dg.asset(
    deps=[nhgis_place_geom_1900_2010_downloaded],
    kinds={'postgres'},
    group_name="usa_raw"
)
def usa_nhgis_census_place_geom_all_years_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    context.log.info(f"Copying census place geom from nhgis to postgres for year")
    census_place_geom_gdf_list = []
    for year in range(1900, 2011, 10):
        census_place_geom_gdf = gpd.read_file(storage.paths.usa.nhgis.census_place_geom(year=year))
        census_place_geom_gdf_list.append(census_place_geom_gdf)

    census_place_geom_gdf = pd.concat(census_place_geom_gdf_list)
    census_place_geom_gdf = gpd.GeoDataFrame(census_place_geom_gdf, geometry='geometry')
    census_place_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_nhgis_census_place_geom_all_years_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="usa_raw",
    io_manager_key="postgres_io_manager"
)
def usa_hist_census_place_geom_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    context.log.info(f"Copying census place geom from hist to postgres")
    census_place_geom_df = pd.read_csv(storage.paths.usa.census_place_project.hist_census_place_geom())
    return census_place_geom_df


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="usa_raw"
)
def usa_states_geom_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    context.log.info(f"Copying states geom from hist to postgres")
    states_geom_gdf = gpd.read_file(storage.paths.usa.misc.usa_state_geom())
    states_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_states_geom_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )