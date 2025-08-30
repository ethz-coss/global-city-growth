import dagster as dg
from dagster_duckdb import DuckDBResource
from dataclasses import dataclass
import jinja2
import os
import pandas as pd
from sqlalchemy import text
import geopandas as gpd
import subprocess
from typing import List

from ..resources.resources import PostgresResource, StorageResource, TableNamesResource
from .constants import constants
from ...utils import union_year_raster_tables_into_single_table


ghsl_years_partition = dg.StaticPartitionsDefinition([str(y) for y in constants['GHSL_RASTER_YEARS']])

def _load_ghsl_raster(context: dg.AssetExecutionContext, bash: dg.PipesSubprocessClient, raster_path: str, table_name: str):
    context.log.info(f"Loading {raster_path} into {table_name}")
    schema_name = 'public'
    args = [
        raster_path,
        schema_name,
        table_name
    ]

    current_dir = os.path.dirname(os.path.abspath(__file__))
    bash_script_path = f'{current_dir}/load_ghsl_raster.sh'
    cmd = ["bash", bash_script_path] + args
    return bash.run(command=cmd, context=context).get_results()

@dg.asset(
    kinds={'postgres'},
    partitions_def=ghsl_years_partition,
    group_name="world_raw"
)
def world_raster_ghsl_pop(context: dg.AssetExecutionContext, storage: StorageResource, tables: TableNamesResource, bash: dg.PipesSubprocessClient):
    year = context.partition_key
    context.log.info(f"Copying GHSL pop raster for year {year}")
    ghsl_pop_raster_path = storage.paths.world.ghsl.pop(year=year)
    table_name = tables.names.world.sources.world_raster_ghsl_pop(year=year)
    return _load_ghsl_raster(context=context, bash=bash, raster_path=ghsl_pop_raster_path, table_name=table_name)

@dg.asset(
    kinds={'postgres'},
    partitions_def=ghsl_years_partition,
    group_name="world_raw"
)
def world_raster_ghsl_smod(context: dg.AssetExecutionContext, storage: StorageResource, tables: TableNamesResource, bash: dg.PipesSubprocessClient):
    year = context.partition_key
    context.log.info(f"Copying GHSL smod raster for year {year}")
    ghsl_smod_raster_path = storage.paths.world.ghsl.smod(year=year)
    table_name = tables.names.world.sources.world_raster_ghsl_smod(year=year)
    return _load_ghsl_raster(context=context, bash=bash, raster_path=ghsl_smod_raster_path, table_name=table_name)


@dg.asset(
    kinds={'postgres'},
    group_name="world_raw"
)
def world_raster_ghsl_pop_all_years(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Copying all GHSL pop rasters for all years in a single table")
    union_year_raster_tables_into_single_table(
        union_table_name=tables.names.world.sources.world_raster_ghsl_pop_all_years(),
        get_year_raster_table_name=tables.names.world.sources.world_raster_ghsl_pop,
        years=constants['GHSL_RASTER_YEARS'],
        context=context,
        con=postgres.get_engine()
    )
    
@dg.asset(
    kinds={'postgres'},
    group_name="world_raw"
)
def world_raster_ghsl_smod_all_years(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Copying all GHSL smod rasters for all years in a single table")
    union_year_raster_tables_into_single_table(
        union_table_name=tables.names.world.sources.world_raster_ghsl_smod_all_years(),
        get_year_raster_table_name=tables.names.world.sources.world_raster_ghsl_smod,
        years=constants['GHSL_RASTER_YEARS'],
        context=context,
        postgres=postgres
    )

@dg.asset(
    kinds={'postgres'},
    group_name="world_raw"
)
def world_country_borders_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    country_borders_path = storage.paths.world.cshapes_border.country_borders()
    context.log.info(f"Copying country borders from {country_borders_path}")

    country_borders_df = gpd.read_file(country_borders_path, engine='pyogrio')
    country_borders_df.to_postgis(
        name=tables.names.world.sources.world_country_borders_raw(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )

@dg.asset(
    kinds={'postgres'},
    group_name="world_raw"
)
def world_crosswalk_cshapes_code_to_iso_code(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    crosswalk_path = storage.paths.world.cshapes_border.crosswalk_cshapes_code_to_iso_code()
    context.log.info(f"Copying crosswalk cshapes code to iso code from {crosswalk_path}")

    crosswalk_df = pd.read_csv(crosswalk_path)
    crosswalk_df.to_sql(
        name=tables.names.world.sources.world_crosswalk_cshapes_code_to_iso_code(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )


@dg.asset(
    kinds={'postgres'},
    group_name="world_raw"
)
def world_urbanization_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    urbanization_path = storage.paths.world.owid.urbanization()
    context.log.info(f"Copying urbanization from {urbanization_path}")

    urbanization_df = pd.read_csv(urbanization_path)
    urbanization_df.to_sql(
        name=tables.names.world.sources.world_urbanization_raw(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )


