from dagster_duckdb import DuckDBResource
import dagster as dg
from dataclasses import dataclass
import jinja2
import os

from ..resources import StorageResource
from src.orchestration.defs.assets.constants import IPUMS_HISTORICAL_YEARS

ipums_historical_years_partitions = dg.StaticPartitionsDefinition([str(y) for y in IPUMS_HISTORICAL_YEARS])


@dataclass(frozen=True)
class TableNameManager:
    def ipums_full_count_table_raw(self, year: int) -> str:
        return f"ipums_full_count_raw_{year}"
    
    def ipums_full_count_table_clean(self, year: int) -> str:
        return f"ipums_full_count_clean_{year}"
    
    def crosswalk_hist_id_to_hist_census_place_table_raw(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_raw_{year}"
    
    def crosswalk_hist_id_to_hist_census_place_table_clean(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_clean_{year}"
    
    def ipums_full_count_census_with_census_place_id(self, year: int) -> str:
        return f"ipums_full_count_census_with_census_place_id_{year}"
    
    def ipums_full_count_census_with_census_place_id_all_years(self) -> str:
        return f"ipums_full_count_census_with_census_place_id_all_years"
    
    def ipums_full_count_individual_migration(self) -> str:
        return f"ipums_full_count_individual_migration"
    
    def census_place_population(self) -> str:
        return f"census_place_population"
    
    def census_place_migration(self) -> str:
        return f"census_place_migration"




def _execute_sql_query_and_return_num_rows(duckdb: DuckDBResource, sql_query: str, table_name: str):
    with duckdb.get_connection() as con:
        con.sql(f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT')}'")
        con.sql(f"SET temp_directory='{os.environ.get('DUCKDB_SPILL_DIRECTORY')}'")
        con.sql(f"SET threads={os.environ.get('DUCKDB_MAX_THREADS')}")
        con.sql(sql_query)
        num_rows = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]    
        
    return num_rows

def _load_table_into_duckdb(duckdb: DuckDBResource, table_name: str, file_path: str):
    sql_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} AS
    SELECT * 
    FROM read_csv('{file_path}');
    """
    return _execute_sql_query_and_return_num_rows(duckdb, sql_query, table_name)


# Define the demographic data asset
@dg.asset(
    kinds={'duckdb'},
    partitions_def=ipums_historical_years_partitions,
    group_name="ipums_full_count_bronze",
    pool="duckdb_write"
)
def ipums_full_count_table_raw(context: dg.AssetExecutionContext, duckdb: DuckDBResource, storage: StorageResource):
    """An asset for the full count census data for a given year (raw)."""
    year = context.partition_key
    table_name = TableNameManager().ipums_full_count_table_raw(year)
    file_path = storage.paths.usa.ipums_full_count.ipums_full_count(year)

    context.log.info(f"Loading IPUMS full count data for {year} into table {table_name}")
    num_rows = _load_table_into_duckdb(duckdb, table_name, file_path)
    context.add_output_metadata({"num_rows": num_rows, "table": table_name})

@dg.asset(
    deps=[ipums_full_count_table_raw],
    kinds={'duckdb'},
    partitions_def=ipums_historical_years_partitions,
    group_name="ipums_full_count_silver",
    pool="duckdb_write"
)
def ipums_full_count_table_clean(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the full count census data for a given year (clean)."""
    year = context.partition_key
    raw_table_name = TableNameManager().ipums_full_count_table_raw(year)
    clean_table_name = TableNameManager().ipums_full_count_table_clean(year)

    context.log.info(f"Cleaning IPUMS full count data for {year} into table {clean_table_name}")

    sql_query = f"""
    DROP TABLE IF EXISTS {clean_table_name};
    CREATE TABLE {clean_table_name} AS
    SELECT  UPPER(histid) AS histid,
            NULLIF(ANY_VALUE(hik), '                     ') AS hik
    FROM { raw_table_name }
    GROUP BY UPPER(histid)
    """

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=clean_table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": clean_table_name})
    

# Define the geographic data asset
@dg.asset(
    kinds={'duckdb'},
    partitions_def=ipums_historical_years_partitions,
    group_name="ipums_full_count_bronze",
    pool="duckdb_write"
)
def crosswalk_hist_id_to_hist_census_place_table_raw(context: dg.AssetExecutionContext, duckdb: DuckDBResource, storage: StorageResource):
    """An asset for the crosswalk between full count census hist_id and census place id for a given year (raw)."""
    year = context.partition_key
    table_name = TableNameManager().crosswalk_hist_id_to_hist_census_place_table_raw(year)
    file_path = storage.paths.usa.census_place_project.crosswalk_hist_id_to_hist_census_place(year)

    context.log.info(f"Loading crosswalk data for {year} into table {table_name}")
    num_rows = _load_table_into_duckdb(duckdb, table_name, file_path)
    context.add_output_metadata({"num_rows": num_rows, "table": table_name})


@dg.asset(
    deps=[crosswalk_hist_id_to_hist_census_place_table_raw],
    kinds={'duckdb'},
    partitions_def=ipums_historical_years_partitions,
    group_name="ipums_full_count_silver",
    pool="duckdb_write"
)
def crosswalk_hist_id_to_hist_census_place_table_clean(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the crosswalk between full count census hist_id and census place id for a given year (clean)."""
    year = context.partition_key
    raw_table_name = TableNameManager().crosswalk_hist_id_to_hist_census_place_table_raw(year)
    clean_table_name = TableNameManager().crosswalk_hist_id_to_hist_census_place_table_clean(year)

    context.log.info(f"Cleaning crosswalk data for {year} into table {clean_table_name}")

    sql_query = f"""
    DROP TABLE IF EXISTS {clean_table_name};
    CREATE TABLE {clean_table_name} AS
    SELECT  UPPER(histid) AS histid,
            ANY_VALUE(cpp_placeid) AS census_place_id
    FROM { raw_table_name }
    GROUP BY UPPER(histid)
    """

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=clean_table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": clean_table_name})

@dg.asset(
    deps=[ipums_full_count_table_clean, crosswalk_hist_id_to_hist_census_place_table_clean],
    kinds={'duckdb'},
    partitions_def=ipums_historical_years_partitions,
    group_name="ipums_full_count_silver",
    pool="duckdb_write"
)  
def ipums_full_count_census_with_census_place_id(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the full count census data with census place id for a given year."""
    year = context.partition_key
    ipums_full_count_table_name = TableNameManager().ipums_full_count_table_clean(year)
    crosswalk_hist_id_to_hist_census_place_table_name = TableNameManager().crosswalk_hist_id_to_hist_census_place_table_clean(year)
    table_name = TableNameManager().ipums_full_count_census_with_census_place_id(year)

    context.log.info(f"Joining IPUMS full count data with census place id for {year}")

    sql_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} AS
    SELECT  histid,
            fcc.hik,
            CASE WHEN cpp.census_place_id > 69491 THEN NULL ELSE cpp.census_place_id END AS census_place_id
    FROM {ipums_full_count_table_name} AS fcc
    LEFT JOIN {crosswalk_hist_id_to_hist_census_place_table_name} AS cpp
    USING (histid)
    WHERE histid IS NOT NULL
    """

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": table_name})
    

@dg.asset(
    deps=[ipums_full_count_census_with_census_place_id],
    kinds={'duckdb'},
    group_name="ipums_full_count_silver",
    pool="duckdb_write"
)
def ipums_full_count_census_with_census_place_id_all_years(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the full count census data with census place id for all years."""
    union_table_name = TableNameManager().ipums_full_count_census_with_census_place_id_all_years()

    context.log.info(f"Joining IPUMS full count data with census place id for all years")

    sql_query_template = """
    DROP TABLE IF EXISTS {{ union_table_name }};
    CREATE TABLE {{ union_table_name }} AS
    {% for table in tables %}
        {% if not loop.first %}
            UNION ALL
        {% endif %}
        SELECT  histid,
                hik,
                census_place_id,
                {{ table.year }} AS year
        FROM {{ table.name }}
    {% endfor %}
    """

    tables = [
        {
            "name": TableNameManager().ipums_full_count_census_with_census_place_id(year),
            "year": year
        }
        for year in IPUMS_HISTORICAL_YEARS
    ]

    template = jinja2.Template(sql_query_template)
    sql_query = template.render(tables=tables, union_table_name=union_table_name)

    context.log.info(f"Query: {sql_query}")

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=union_table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": union_table_name})

@dg.asset(
    deps=[ipums_full_count_census_with_census_place_id_all_years],
    kinds={'duckdb'},
    group_name="ipums_full_count_silver",
    pool="duckdb_write"
)
def ipums_full_count_individual_migration(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the individual migration data for all years."""
    ipums_full_count_census_with_census_place_id_all_years_table_name = TableNameManager().ipums_full_count_census_with_census_place_id_all_years()
    table_name = TableNameManager().ipums_full_count_individual_migration()

    context.log.info(f"Calculating individual migration data for all years")

    sql_query_template = """
    DROP TABLE IF EXISTS {{ migration_table_name }};
    CREATE TABLE {{ migration_table_name }} AS
    WITH
    {% for i in range(1, years | length) %}
        {% set y1 = years[i-1] %}
        {% set y2 = years[i] %}
        migration_{{y1}}_{{y2}} AS (
            SELECT  hik,
                    cy1.census_place_id AS census_place_origin,
                    cy1.year AS year_origin,
                    cy2.census_place_id AS census_place_destination,
                    cy2.year AS year_destination
            FROM (
                SELECT hik, census_place_id, year
                FROM {{ census_table_name }}
                WHERE year = {{ y1 }}
            ) cy1
            JOIN (
                SELECT hik, census_place_id, year
                FROM {{ census_table_name }}
                WHERE year = {{ y2 }}
            ) cy2 USING (hik)
            WHERE cy1.census_place_id IS NOT NULL
            AND cy2.census_place_id IS NOT NULL
        ),
    {% endfor %}
    migration AS (
        {% set statements = [] %}
        {% for i in range(1, years | length) %}
            {% set y1 = years[i-1] %}
            {% set y2 = years[i] %}
            {% set stmt = "SELECT * FROM migration_" ~ y1 ~ "_" ~ y2 %}
            {% do statements.append(stmt) %}
        {% endfor %}
        {{ statements | join(' UNION ALL \n') }}
    )
    SELECT * FROM migration
    """
    # Add 'do' extension to jinja2 for loops
    env = jinja2.Environment(extensions=['jinja2.ext.do'])
    template = env.from_string(sql_query_template)
    sql_query = template.render(migration_table_name=table_name, census_table_name=ipums_full_count_census_with_census_place_id_all_years_table_name, years=sorted(IPUMS_HISTORICAL_YEARS))

    context.log.info(f"Query: {sql_query}")

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": table_name})
    
    
@dg.asset(
    deps=[ipums_full_count_census_with_census_place_id_all_years],
    kinds={'duckdb'},
    group_name="ipums_full_count_gold",
    pool="duckdb_write"
)
def census_place_population(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the census place population for all years."""
    census_place_population_table_name = TableNameManager().census_place_population()
    ipums_full_count_census_with_census_place_id_all_years_table_name = TableNameManager().ipums_full_count_census_with_census_place_id_all_years()

    context.log.info(f"Calculating census place population for all years")

    sql_query = f"""
    DROP TABLE IF EXISTS {census_place_population_table_name};
    CREATE TABLE {census_place_population_table_name} AS
    SELECT  census_place_id,
            year,
            COUNT(*) AS population
    FROM {ipums_full_count_census_with_census_place_id_all_years_table_name}
    WHERE census_place_id IS NOT NULL
    GROUP BY census_place_id, year
    """

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=census_place_population_table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": census_place_population_table_name})


@dg.asset(
    deps=[ipums_full_count_individual_migration],
    kinds={'duckdb'},
    group_name="ipums_full_count_gold",
    pool="duckdb_write"
)
def census_place_migration(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """An asset for the census place migration for all years."""
    ipums_full_count_individual_migration_table_name = TableNameManager().ipums_full_count_individual_migration()
    census_place_migration_table_name = TableNameManager().census_place_migration()

    context.log.info(f"Calculating census place migration for all years")

    sql_query = f"""
    DROP TABLE IF EXISTS {census_place_migration_table_name};
    CREATE TABLE {census_place_migration_table_name} AS
    SELECT  census_place_origin,
        census_place_destination,
        year_origin,
        year_destination,
        COUNT(*) as all_migrants
    FROM {ipums_full_count_individual_migration_table_name}
    GROUP BY census_place_origin, census_place_destination, year_origin, year_destination
    """

    num_rows = _execute_sql_query_and_return_num_rows(duckdb=duckdb, sql_query=sql_query, table_name=census_place_migration_table_name)
    context.add_output_metadata({"num_rows": num_rows, "table": census_place_migration_table_name})