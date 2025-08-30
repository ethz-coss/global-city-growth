import dagster as dg
from dataclasses import dataclass
import pandas as pd
import networkx as nx 
from typing import List, Tuple
from sqlalchemy import text
import numpy as np
import rioxarray as riox
from rasterio.io import MemoryFile
import sqlalchemy
import xarray as xr
import scipy.ndimage as ndimage

from ..resources.resources import PostgresResource, TableNamesResource
from ...utils import union_year_raster_tables_into_single_table, extract_connected_components, crosswalk_component_id_to_cluster_id
from .constants import constants
    

def _load_raster(con: sqlalchemy.Connection, table_name: str, year: int, schema: str = 'public') -> xr.DataArray:
    res = con.execute(text(f"""
                SELECT ST_AsGDALRaster(ST_Union(rast), 'GTIff') 
                FROM {schema}.{table_name}
                WHERE year = {year}
                """))
    raster = res.fetchone()
    in_memory_raster = MemoryFile(bytes(raster[0]))
    raster_dataset = riox.open_rasterio(in_memory_raster)
    return raster_dataset

def _dump_raster(con: sqlalchemy.Connection, data: xr.DataArray, table_name: str, schema: str = 'public'):
    assert data.rio is not None, "The input data must be a rioxarray DataArray"
    assert data.rio.crs is not None, "The input data must have a CRS"
    assert data.rio.transform() is not None, "The input data must have a transform"

    raster_array = data.rio
    width, height = raster_array.width, raster_array.height
    bands = raster_array.count
    srid = raster_array.crs.to_epsg()
    nodata = raster_array.nodata
    with MemoryFile() as memory_file:
        with memory_file.open(driver='GTiff', width=width, height=height, count=bands, dtype=raster_array._obj.dtype, crs=f'EPSG:{srid}', transform=raster_array.transform(), nodata=nodata) as dataset:
            dataset.write(data)

        geotiff_data = memory_file.read()

    con.execute(sqlalchemy.text(f"CREATE TABLE {schema}.{table_name} (rast raster);"))
    con.execute(sqlalchemy.text(f"INSERT INTO {schema}.{table_name} (rast) VALUES (ST_FromGDALRaster(:data))"), {'data': geotiff_data})
    con.execute(sqlalchemy.text(f"SELECT AddRasterConstraints('{schema}'::name, '{table_name}'::name, 'rast'::name);"))

def _get_2d_exponential_kernel(size: int, decay_rate: float) -> np.ndarray:
    """
    Create a 2D exponential kernel

    Parameters:
    - size: size of the kernel
    - sigma: standard deviation of the kernel

    Returns:
    - A 2D numpy array representing the kernel
    """
    assert size % 2 == 1, "The size of the kernel must be odd"
    indices = np.arange(size) - (size - 1) / 2
    x, y = np.meshgrid(indices, indices)
    distance_grid = np.sqrt(x ** 2 + y ** 2)
    kernel = np.exp(-decay_rate * distance_grid)
    return kernel / np.sum(kernel)


def _convolve2d(image, kernel):
    return ndimage.convolve(image, kernel, mode='constant', cval=0.0)


@dg.asset(
    deps=[TableNamesResource().names.usa.transformations.usa_nhgis_census_place_equivalence_network()],
    kinds={'postgres'},
    group_name="usa_intermediate_normalize_hist_data"
)
def usa_crosswalk_nhgis_census_place_to_connected_component(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Creating crosswalk from NHGIS census place to connected component of equivalent census places")

    q = f"""
    SELECT left_census_place_id, right_census_place_id
    FROM {tables.names.usa.transformations.usa_nhgis_census_place_equivalence_network()}
    """
    edges = pd.read_sql(q, con=postgres.get_engine())
    connected_components_df = extract_connected_components(edges_df=edges, left_col="left_census_place_id", right_col="right_census_place_id", output_id_col="census_place_id")

    connected_components_df.to_sql(
        name=tables.names.usa.transformations.usa_crosswalk_nhgis_census_place_to_connected_component(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )

@dg.asset(
    deps=[TableNamesResource().names.usa.transformations.usa_raster_census_place()],
    partitions_def=dg.StaticPartitionsDefinition([str(y) for y in constants["USA_YEARS"]]),
    kinds={'postgres'},
    group_name="usa_intermediate_rasterize_census_places"
)
def usa_raster_census_place_convolved_year(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    year = context.partition_key
    context.log.info(f"Convolving raster census place for year {year}")

    convolution_kernel_size = constants["USA_CONVOLUTION_KERNEL_SIZE"]
    convolution_kernel_decay = constants["USA_CONVOLUTION_KERNEL_DECAY"]
    convolved_raster_table_name = tables.names.usa.transformations.usa_raster_census_place_convolved_year(year=year)

    engine = postgres.get_engine()
    with engine.begin() as con:
        con.execute(text(f"DROP TABLE IF EXISTS {convolved_raster_table_name}"))
        raster = _load_raster(con=con, table_name=tables.names.usa.transformations.usa_raster_census_place(), year=year)
        raster_vals = raster.sel(band=1).values.astype(np.float64)

        kernel = _get_2d_exponential_kernel(size=convolution_kernel_size, decay_rate=convolution_kernel_decay)
        convolved_raster_vals = _convolve2d(image=raster_vals, kernel=kernel)

        convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
        _dump_raster(con=con, data=convolved_raster, table_name=convolved_raster_table_name, schema="public")


@dg.asset(
    deps=[usa_raster_census_place_convolved_year],
    kinds={'postgres'},
    group_name="usa_intermediate_rasterize_census_places"
)
def usa_raster_census_place_convolved_all_years(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Unioning all the convolved raster tables into a single table")
    union_year_raster_tables_into_single_table(
        union_table_name=tables.names.usa.transformations.usa_raster_census_place_convolved_all_years(),
        get_year_raster_table_name=tables.names.usa.transformations.usa_raster_census_place_convolved_year,
        years=constants["USA_YEARS"],
        context=context,
        postgres=postgres
    )

def _year_pairs(years: List[int]) -> List[Tuple[int, int]]:
    return [(y1, y2) for y1 in years for y2 in years if y1 <= y2]

@dg.asset(
    deps=[TableNamesResource().names.usa.transformations.usa_cluster_base_matching()],
    kinds={'postgres'},
    group_name="usa_intermediate_create_clusters"
)
def usa_crosswalk_component_id_to_cluster_id(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    context.log.info(f"Creating cluster base matching")
    urban_thresholds = constants["USA_URBAN_POPULATION_PIXEL_THRESHOLDS"]
    engine = postgres.get_engine()
    with engine.begin() as con:
        q = f"""
        SELECT DISTINCT y1, y2
        FROM {tables.names.usa.transformations.usa_cluster_base_matching()}
        """
        year_pairs = pd.read_sql(q, con=con)
        year_pairs = [(row['y1'], row['y2']) for _, row in year_pairs.iterrows()]

    years_threshold_pairs = []
    for ut in urban_thresholds:
        for y1, y2 in year_pairs:
            years_threshold_pairs.append((y1, y2, ut))


    crosswalk_component_id_to_cluster_id(
        years_threshold_pairs=years_threshold_pairs,
        matching_table_name=tables.names.usa.transformations.usa_cluster_base_matching(),
        crosswalk_table_name=tables.names.usa.transformations.usa_crosswalk_component_id_to_cluster_id(),
        context=context,
        postgres=postgres
    )


    
