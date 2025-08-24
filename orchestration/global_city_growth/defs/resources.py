from dagster_duckdb import DuckDBResource
from dagster_postgres import PostgresResource
from dagster import ConfigurableResource
from pathlib import Path
import dagster as dg

from configs.paths import DataPaths

class StorageResource(ConfigurableResource):
    """A resource for accessing file system paths."""
    data_root: Path

    @property
    def paths(self) -> DataPaths:
        return DataPaths.from_root(self.data_root)


postgres_resource = PostgresResource(
    user={"env": "POSTGRES_USER"},
    password={"env": "POSTGRES_PASSWORD"},
    host="postgres_db", # The service name in docker-compose
    db_name={"env": "POSTGRES_DB"},
    port=5432,
)

duckdb_resource = DuckDBResource(
    database="tmp/local.duckdb"
)

storage_resource = StorageResource(
    data_root=Path({"env": "DATA_ROOT_PATH"})
)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "storage": storage_resource,
            "postgres": postgres_resource,
            "duckdb": duckdb_resource,
        }
    )