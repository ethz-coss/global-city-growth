import dagster as dg
import pandas as pd

from ...resources.resources import PostgresResource, TableNamesResource
from ....utils import crosswalk_component_id_to_cluster_id
from ..constants import constants


@dg.asset(
    deps=[TableNamesResource().names.world.transformations.world_cluster_base_matching()],
    kinds={'postgres'},
    group_name="world_intermediate",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="component_id", type="INT", description="The ID of the connected component"),
            dg.TableColumn(name="cluster_id", type="INT", description="See world_cluster_growth_geom"),
            dg.TableColumn(name="y1", type="INT", description="See world_cluster_growth_geom"),
            dg.TableColumn(name="y2", type="INT", description="See world_cluster_growth_geom"),
            dg.TableColumn(name="urban_threshold", type="INT", description="see world_cluster_base_geom"),
        ])
    }

)
def world_crosswalk_component_id_to_cluster_id(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    """Crosswalk mapping each cluster to its connected component in the bipartite graph matching across years. Each connected component correpsonds to a unique cluster growth. """
    
    context.log.info(f"Creating matching between connected components and cluster ids")
    urban_thresholds = constants["WORLD_DEGREE_OF_URBANIZATION_THRESHOLDS"]

    engine = postgres.get_engine()
    with engine.begin() as con:
        q = f"""
        SELECT DISTINCT y1, y2
        FROM {tables.names.world.transformations.world_cluster_base_matching()}
        """
        year_pairs = pd.read_sql(q, con=con)
        year_pairs = [(row['y1'], row['y2']) for _, row in year_pairs.iterrows()]
    
    years_threshold_pairs = []
    for ut in urban_thresholds:
        for y1, y2 in year_pairs:
            years_threshold_pairs.append((y1, y2, ut))

    crosswalk_component_id_to_cluster_id(
        years_threshold_pairs=years_threshold_pairs,
        matching_table_name=tables.names.world.transformations.world_cluster_base_matching(),
        crosswalk_table_name=tables.names.world.transformations.world_crosswalk_component_id_to_cluster_id(),
        context=context,
        postgres=postgres
    )
