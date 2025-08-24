from dagster_duckdb import DuckDBResource
from dagster_postgres import PostgresResource


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