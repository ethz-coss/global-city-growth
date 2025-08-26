#!/bin/bash
set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Add extensions
"${psql[@]}" --dbname="$POSTGRES_DB" <<- EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
	CREATE EXTENSION IF NOT EXISTS postgis_raster;
    ALTER DATABASE "$POSTGRES_DB" SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
EOSQL