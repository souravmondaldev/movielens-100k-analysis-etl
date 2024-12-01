#!/bin/bash
# Initialize the Airflow database (if not already initialized)
airflow db init

# Create the admin user
airflow users create \
  --role Admin \
  --username admin \
  --email admin@example.com \
  --firstname admin \
  --lastname admin \
  --password admin
