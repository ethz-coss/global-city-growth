import jinja2
from typing import List, Callable, Tuple
import dagster as dg
from sqlalchemy import text
import pandas as pd
import networkx as nx

from .defs.resources.resources import PostgresResource, TableNamesResource


def union_year_raster_tables_into_single_table(union_table_name: str, get_year_raster_table_name: Callable[[int], str], years: List[int], context: dg.AssetExecutionContext, postgres: PostgresResource):
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
            "name": get_year_raster_table_name(year=year),
            "year": year
        }
        for year in years
    ]

    sql_query = template.render(tables=tables, table_name=union_table_name)
    context.log.info(f"Query: {sql_query}")

    with postgres.get_engine().begin() as con:
        con.execute(text(sql_query))

def extract_connected_components(edges_df: pd.DataFrame, left_col: str, right_col: str, output_id_col: str) -> pd.DataFrame:
    edges = [(str(row[left_col]), str(row[right_col])) for _, row in edges_df.iterrows()]
    network = nx.Graph()
    network.add_edges_from(edges)

    connected_components = list(nx.connected_components(network))

    crosswalk = []
    for i, component in enumerate(connected_components):
        for node in component:
            crosswalk.append({'component_id': i, output_id_col: node})

    return pd.DataFrame(crosswalk)


def crosswalk_component_id_to_cluster_id(years_threshold_pairs: List[Tuple[int, int, int]], matching_table_name: str, crosswalk_table_name: str, context: dg.AssetExecutionContext, postgres: PostgresResource):
    context.log.info(f"Creating cluster base matching")
    engine = postgres.get_engine()
    crosswalk = []
    for y1, y2, ut in years_threshold_pairs:
        context.log.info(f"Creating cluster base matching for {y1} and {y2} with urban pixel threshold {ut}")

        q = f"""
        SELECT left_cluster_id, COALESCE(right_cluster_id, left_cluster_id) as right_cluster_id
        FROM {matching_table_name}
        WHERE y1 = {y1} AND y2 = {y2} AND urban_threshold = {ut}
        """

        edges = pd.read_sql(q, con=engine)
        connected_components_df = extract_connected_components(edges_df=edges, left_col="left_cluster_id", right_col="right_cluster_id", output_id_col="cluster_id")

        connected_components_df['y1'] = y1
        connected_components_df['y2'] = y2
        connected_components_df['urban_threshold'] = ut

        crosswalk.append(connected_components_df)

    crosswalk = pd.concat(crosswalk)
    crosswalk.to_sql(
        name=crosswalk_table_name,
        con=engine,
        if_exists="replace",
        index=False,
        schema="public"
    )