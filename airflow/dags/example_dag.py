from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG('example_spark_job_dag', default_args=default_args, schedule_interval=None) as dag:
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='docker exec spark spark-submit /app/spark_job.py'
    )
