import dagster as dg
from dataclasses import dataclass
import pandas as pd
import networkx as nx 
from typing import List

from ..resources.resources import PostgresResource


@dataclass(frozen=True)
class TableNameManager:
    def usa_nhgis_census_place_equivalence_network(self) -> str:
        return "usa_nhgis_census_place_equivalence_network"
    
    def usa_crosswalk_nhgis_census_place_to_stable_census_place(self) -> str:
        return "usa_crosswalk_nhgis_census_place_to_stable_census_place"
    

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
def usa_match_equivalent_census_places(context: dg.AssetExecutionContext, postgres: PostgresResource):
    context.log.info(f"Matching equivalent census places")

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
        name=TableNameManager().usa_crosswalk_nhgis_census_place_to_stable_census_place(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )