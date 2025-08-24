from dagster_duckdb import DuckDBResource
from dagster_postgres import PostgresResource
import dagster as dg

from .resources import StorageResource


@dg.asset(
    required_resource_keys={"storage", "postgres", "duckdb"},
    io_manager_key="storage",
)
def load_usa_state_geom(context: dg.AssetExecutionContext) -> None:
    pass








