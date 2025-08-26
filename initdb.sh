#!/bin/bash
set -euxo pipefail

# Create the two databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
  CREATE DATABASE "${POSTGRES_DB_DAGSTER}";
  CREATE DATABASE "${POSTGRES_DB_MAIN}";
EOSQL

# Enable PostGIS in your main app DB only
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB_MAIN" <<-EOSQL
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS postgis_raster;
  ALTER DATABASE "$POSTGRES_DB_MAIN" SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
EOSQL