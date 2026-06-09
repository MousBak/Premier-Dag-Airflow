#!/bin/bash
# Creates the weather data database alongside the Airflow metadata database.
# Runs once at PostgreSQL container startup.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE meteo_db;
    GRANT ALL PRIVILEGES ON DATABASE meteo_db TO $POSTGRES_USER;
EOSQL

echo "Database meteo_db created."
