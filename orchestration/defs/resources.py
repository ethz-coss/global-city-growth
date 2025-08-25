from dagster_duckdb import DuckDBResource
from dagster import ConfigurableResource
from pathlib import Path
import dagster as dg
import os

from configs.paths import DataPaths

class StorageResource(ConfigurableResource):
    """A resource for accessing file system paths."""
    data_root: str

    @property
    def paths(self) -> DataPaths:
        return DataPaths.from_root(Path(self.data_root))

duckdb_resource = DuckDBResource(
    database="/app/tmp/temporary_duckdb.db"
)

storage_resource = StorageResource(
    data_root=dg.EnvVar("DATA_ROOT_PATH")
)