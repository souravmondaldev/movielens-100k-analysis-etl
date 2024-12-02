#!/bin/bash

# Wait for the webserver to be ready
airflow db check

airflow connections delete postgres_default
echo "Delete current postgres_default connections with None port"

airflow connections delete spark_default
echo "Delete current spark_default connections with None port"

airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-host 'postgres' \
    --conn-port 5432 \
    --conn-schema 'airflow'

echo "Connection postgres_default added successfully"


airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark' \
    --conn-port 7077
echo "Connection spark_default added successfully"
