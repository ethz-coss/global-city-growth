from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtProject, DbtCliResource
from dagster import ConfigurableResource, ConfigurableIOManager
from pathlib import Path
import dagster as dg
import sqlalchemy
import os
import pandas as pd
from pydantic import PrivateAttr
import json

from .paths import DataPaths
from .tables import TableNames
from ..assets.constants import constants
from .ipums_api import IpumsAPIClient

class StorageResource(ConfigurableResource):
    """A resource for accessing file system paths."""
    root: str

    @property
    def paths(self) -> DataPaths:
        return DataPaths.from_root(Path(self.root))

    @property
    def data_root(self) -> Path:
        return Path(self.root)

    @property
    def data_catalog_path(self) -> Path:
        return Path('data_catalog.yml')
    
class TableNamesResource(ConfigurableResource):
    """A resource for accessing tables."""
    
    @property
    def names(self) -> TableNames:
        return TableNames()


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


class PostgresPandasIOManager(ConfigurableIOManager):
    postgres: PostgresResource

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame):
        context.log.info(f"Saving {obj.shape[0]} rows to {context.asset_key.path}")
        table_name = context.asset_key.path[-1]
        obj.to_sql(
            name=table_name,
            con=self.postgres.get_engine(),
            schema='public',
            index=False,
            if_exists='replace'
        )
    
    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        table_name = context.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.postgres.get_engine())
        return df


class IpumsAPIClientResource(ConfigurableResource):
    api_key: str
    
    def get_client(self) -> IpumsAPIClient:
        return IpumsAPIClient(self.api_key)


dbt_project = DbtProject(
  project_dir='src/warehouse'
)

dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

duckdb_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_PATH")
)

storage_resource = StorageResource(
    root=dg.EnvVar("DATA_ROOT_PATH")
)

table_names_resource = TableNamesResource()

pipes_subprocess_resource = dg.PipesSubprocessClient()

postgres_resource = PostgresResource(
    host=dg.EnvVar("POSTGRES_HOST"),
    port=dg.EnvVar("POSTGRES_PORT"),
    user=dg.EnvVar("POSTGRES_USER"),
    password=dg.EnvVar("POSTGRES_PASSWORD"),
    database=dg.EnvVar("POSTGRES_DB_MAIN")
)

postgres_pandas_io_manager = PostgresPandasIOManager(
    postgres=postgres_resource
)

ipums_api_client = IpumsAPIClientResource(
    api_key=dg.EnvVar("IPUMS_API_KEY")
)