import dagster as dg
from dagster_duckdb import DuckDBResource
from dataclasses import dataclass
import jinja2
import os
import pandas as pd
from sqlalchemy import text
import geopandas as gpd

from ..resources.resources import PostgresResource, StorageResource
from ..assets.ipums_full_count_census import TableNameManager as IpumsFullCountTableNameManager
from ..assets.ipums_full_count_census import census_place_migration as census_place_migration_duckdb, census_place_population as census_place_population_duckdb


class SourceTableNameManager:
    def usa_hist_census_place_population(self) -> str:
        return "usa_hist_census_place_population"
    
    def usa_hist_census_place_migration(self) -> str:
        return "usa_hist_census_place_migration"
    
    def usa_nhgis_census_place_population_1990_2020(self) -> str:
        return "usa_nhgis_census_place_population_1990_2020"
    
    def usa_nhgis_census_place_geom_raw(self, year: int) -> str:
        return f"usa_nhgis_census_place_geom_raw_{year}"
    
    def usa_hist_census_place_geom(self) -> str:
        return "usa_hist_census_place_geom"
    
    def usa_states_geom(self) -> str:
        return "usa_states_geom"


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
    group_name="usa_bronze",
    pool="duckdb_write"
)
def usa_hist_census_place_population(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource):
    context.log.info(f"Copying census place population from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=IpumsFullCountTableNameManager().census_place_population(),
        table_postgres=SourceTableNameManager().usa_hist_census_place_population(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    deps=[census_place_migration_duckdb],
    kinds={'postgres'},
    group_name="usa_bronze",
    pool="duckdb_write"
)
def usa_hist_census_place_migration(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource):
    context.log.info(f"Copying census place migration from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=IpumsFullCountTableNameManager().census_place_migration(),
        table_postgres=SourceTableNameManager().usa_hist_census_place_migration(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    kinds={'postgres'},
    group_name="usa_bronze"
)
def usa_nhgis_census_place_population_1990_2020(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource):
    context.log.info(f"Copying census place population from nhgis to postgres")
    data = pd.read_csv(storage.paths.usa.nhgis.census_place_pop_1990_2020(), encoding='latin1')
    data.to_sql(
        name=SourceTableNameManager().usa_nhgis_census_place_population_1990_2020(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )

@dg.asset(
    kinds={'postgres'},
    group_name="usa_bronze",
    partitions_def=dg.StaticPartitionsDefinition([str(y) for y in range(1900, 2011, 10)])
)
def usa_nhgis_census_place_geom(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource):
    year = int(context.partition_key)
    context.log.info(f"Copying census place geom from nhgis to postgres for year {year}")
    census_place_geom_gdf = gpd.read_file(storage.paths.usa.nhgis.census_place_geom(year=year))
    census_place_geom_gdf.to_postgis(
        name=SourceTableNameManager().usa_nhgis_census_place_geom_raw(year=year),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    kinds={'postgres'},
    group_name="usa_bronze"
)
def usa_hist_census_place_geom(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource):
    context.log.info(f"Copying census place geom from hist to postgres")
    census_place_geom_df = pd.read_csv(storage.paths.usa.census_place_project.hist_census_place_geom())
    census_place_geom_df.to_sql(
        name=SourceTableNameManager().usa_hist_census_place_geom(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )



@dg.asset(
    kinds={'postgres'},
    group_name="usa_bronze"
)
def usa_states_geom(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource):
    context.log.info(f"Copying states geom from hist to postgres")
    states_geom_gdf = gpd.read_file(storage.paths.usa.misc.usa_state_geom())
    states_geom_gdf.to_postgis(
        name=SourceTableNameManager().usa_states_geom(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )