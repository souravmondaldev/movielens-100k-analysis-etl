#!/bin/bash

# Wait for the webserver to be ready
airflow db check

# Add Spark connection
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark' \
    --conn-port 7077

echo "Connections added successfully"
