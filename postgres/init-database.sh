#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER docker;
    CREATE DATABASE user_db;
    GRANT ALL PRIVILEGES ON DATABASE user_db TO docker;
    CREATE TABLE IF NOT EXISTS user_table (
        user_id INT PRIMARY KEY,
        age INT,
        gender VARCHAR(10),
        occupation VARCHAR(50),
        zip_code VARCHAR(10)
    );
EOSQL