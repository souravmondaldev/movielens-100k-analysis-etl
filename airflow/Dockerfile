# Base image
FROM apache/airflow:2.7.3

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Switch to 'airflow' user for installing dependencies
USER airflow

# Install dependencies for PostgreSQL and Spark
#RUN pip install --no-cache-dir psycopg2-binary
#RUN pip install --no-cache-dir pyspark

# Copy requirements file and install dependencies
COPY requirements.txt /requirements.txt

# Install the required Python packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy local DAGs to container
COPY ./dags $AIRFLOW_HOME/dags

# Ensure we continue to use the 'airflow' user for running Airflow
USER airflow
