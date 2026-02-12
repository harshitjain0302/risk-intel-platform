#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE mlflow;
    CREATE DATABASE warehouse;
    GRANT ALL PRIVILEGES ON DATABASE mlflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE warehouse TO airflow;
EOSQL