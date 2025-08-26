from dagster_duckdb import DuckDBResource
from dagster import ConfigurableResource
from pathlib import Path
import dagster as dg
import sqlalchemy
import os

from src.orchestration.defs.paths import DataPaths

class StorageResource(ConfigurableResource):
    """A resource for accessing file system paths."""
    data_root: str

    @property
    def paths(self) -> DataPaths:
        return DataPaths.from_root(Path(self.data_root))


class PostgresResource(ConfigurableResource):
    """A resource for accessing a postgres database."""
    host: str
    port: int
    user: str
    password: str
    database: str

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        return self.engine
    


duckdb_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_PATH")
)

storage_resource = StorageResource(
    data_root=dg.EnvVar("DATA_ROOT_PATH")
)

postgres_resource = PostgresResource(
    host=dg.EnvVar("POSTGRES_HOST"),
    port=dg.EnvVar("POSTGRES_PORT"),
    user=dg.EnvVar("POSTGRES_USER"),
    password=dg.EnvVar("POSTGRES_PASSWORD"),
    database=dg.EnvVar("POSTGRES_DB")
)