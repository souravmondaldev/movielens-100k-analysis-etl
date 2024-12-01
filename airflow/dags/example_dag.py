# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime
#
# default_args = {
#     'start_date': datetime(2024, 1, 1),
#     'catchup': False,
# }
#
# with DAG('example_spark_job_dag', default_args=default_args, schedule_interval=None) as dag:
#     run_spark_job = BashOperator(
#         task_id='run_spark_job',
#         bash_command='/usr/bin/docker exec spark spark-submit /app/spark_job.py'
#     )
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Default arguments
default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

# Define the DAG
with DAG('example_spark_job_dag', default_args=default_args, schedule_interval=None) as dag:

    # Define the SparkSubmitOperator task
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/app/spark_job.py',  # Path to your Spark job script
        conn_id='spark_default',          # Connection ID for your Spark cluster (if using Airflow connections)
        conf={
            "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.2.23.jar",  # Path to the PostgreSQL JDBC driver
        },
        name="SparkJob",                  # Name of the Spark job
        application_args=[],              # Any additional arguments for the Spark job (if needed)
        verbose=True,                     # Whether to show verbose logging
    )

    # Set task execution order (if you have other tasks)
    run_spark_job
