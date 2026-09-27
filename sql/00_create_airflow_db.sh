#!/bin/bash
# Runs once on first Postgres start: create the separate Airflow metadata database.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB};
EOSQL
