from dagster_duckdb import DuckDBResource
from dagster import ConfigurableResource
from pathlib import Path
import dagster as dg
import sqlalchemy
import os
from pydantic import PrivateAttr

from .paths import DataPaths

class StorageResource(ConfigurableResource):
    """A resource for accessing file system paths."""
    data_root: str

    @property
    def paths(self) -> DataPaths:
        return DataPaths.from_root(Path(self.data_root))


class PostgresResource(ConfigurableResource):
    """Postgres access via SQLAlchemy."""
    host: str
    port: str
    user: str
    password: str
    database: str

    # Private, mutable state for the run:
    _engine: sqlalchemy.engine.Engine | None = PrivateAttr(default=None)

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def sqlalchemy_connection_string(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self) -> sqlalchemy.engine.Engine:
        # Lazy init if not set up yet
        if self._engine is None:
            self._engine = sqlalchemy.create_engine(self.sqlalchemy_connection_string)
        return self._engine
    


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