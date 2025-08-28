import dagster as dg
from dataclasses import dataclass
import pandas as pd
import networkx as nx 
from typing import List
from sqlalchemy import text
import numpy as np
import jinja2
import rioxarray as riox
from rasterio.io import MemoryFile
import sqlalchemy
import xarray as xr

from ...utils.convolution import get_2d_exponential_kernel, convolve2d
from ..resources.resources import PostgresResource
from .constants import constants

@dataclass(frozen=True)
class TableNameManager:
    def usa_nhgis_census_place_equivalence_network(self) -> str:
        return "usa_nhgis_census_place_equivalence_network"
    
    def usa_crosswalk_nhgis_census_place_to_connected_component(self) -> str:
        return "usa_crosswalk_nhgis_census_place_to_connected_component"
    
    def usa_raster_census_place(self) -> str:
        return "usa_raster_census_place"
    
    def usa_raster_census_place_convolved(self) -> str:
        return f"usa_raster_census_place_convolved"

def _extract_as_dataframe(connected_components: List[List[str]]) -> pd.DataFrame:
    data = []
    for i, component in enumerate(connected_components):
        for node in component:
            data.append({'component_id': i, 'census_place_id': node})

    data = pd.DataFrame(data)
    return data


@dg.asset(
    deps=[TableNameManager().usa_nhgis_census_place_equivalence_network()],
    kinds={'postgres'},
    group_name="usa_intermediate_normalize_hist_data"
)
def usa_crosswalk_nhgis_census_place_to_connected_component(context: dg.AssetExecutionContext, postgres: PostgresResource):
    context.log.info(f"Creating crosswalk from NHGIS census place to connected component of equivalent census places")

    q = f"""
    SELECT left_census_place_id, right_census_place_id
    FROM {TableNameManager().usa_nhgis_census_place_equivalence_network()}
    """
    edges = pd.read_sql(q, con=postgres.get_engine())
    network = nx.Graph()
    edges = [(f"{row['left_census_place_id']}", f"{row['right_census_place_id']}") for i, row in edges.iterrows()]
    network.add_edges_from(edges)

    connected_components = list(nx.connected_components(network))
    connected_components_df = _extract_as_dataframe(connected_components)

    connected_components_df.to_sql(
        name=TableNameManager().usa_crosswalk_nhgis_census_place_to_connected_component(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )



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
    

@dg.asset(
    deps=[TableNameManager().usa_raster_census_place()],
    kinds={'postgres'},
    group_name="usa_intermediate_rasterize_census_places"
)
def usa_raster_census_place_convolved(context: dg.AssetExecutionContext, postgres: PostgresResource):
    context.log.info(f"Convolving raster census place")
    convolution_kernel_size = constants["USA_CONVOLUTION_KERNEL_SIZE"]
    convolution_kernel_decay = constants["USA_CONVOLUTION_KERNEL_DECAY"]
    usa_years = constants["USA_YEARS"]
    convolved_table_name = TableNameManager().usa_raster_census_place_convolved()
    temp_year_table_name = 'temp_raster_convolved_census_place'

    engine = postgres.get_engine()
    with engine.begin() as con:
        for year in usa_years:
            context.log.info(f"Convolving raster census place for year {year}")
            con.execute(text(f"DROP TABLE IF EXISTS {temp_year_table_name}_{year}"))

            raster = _load_raster(con=con, table_name=TableNameManager().usa_raster_census_place(), year=year)
            raster_vals = raster.sel(band=1).values.astype(np.float64)

            kernel = get_2d_exponential_kernel(size=convolution_kernel_size, decay_rate=convolution_kernel_decay)
            convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)

            convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
            _dump_raster(con=con, data=convolved_raster, table_name=f'{temp_year_table_name}_{year}', schema="public")


        context.log.info(f"Creating final raster table by unioning all the temp tables")
        sql_query = """
        DROP TABLE IF EXISTS {{ table_name }};
        CREATE TABLE {{ table_name }} AS
        {% for table in tables %}
            SELECT  rast,
                    {{ table.year }} AS year
            FROM {{ table.name }}
            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
        """

        template = jinja2.Template(sql_query)
        tables = [
            {
                "name": f"{temp_year_table_name}_{year}",
                "year": year
            }
            for year in usa_years
        ]

        sql_query = template.render(tables=tables, table_name=convolved_table_name)
        context.log.info(f"Query: {sql_query}")
        con.execute(text(sql_query))
        for year in usa_years:
            con.execute(text(f"DROP TABLE {temp_year_table_name}_{year}"))