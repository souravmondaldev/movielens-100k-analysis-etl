import pandas as pd
from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.utils.context import Context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from custom_schedule.working_day_schedule_timetable import WorkingDayScheduleTimeTable


default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}
working_day_timetable = WorkingDayScheduleTimeTable(14, 30) #8P.M IST CONVERTED IN UTC 2:30 PM


logger = logging.getLogger(__name__)
TIMEOUT_IN_12_HOURS = 43200
POKE_INTERVAL_10_MINS = 600
os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk8u292-b10'

def get_formatted_execution_date(**kwargs):
    execution_date = kwargs['execution_date']
    return execution_date.strftime('%Y-%m-%d')

with DAG('pipeline2_analyse_ml_100k_data', 
         default_args=default_args, 
         schedule_interval=None,
         timetable=working_day_timetable) as dag:

    wait_for_pipeline_1_completion_precondtion=ExternalTaskSensor(
        task_id='WAIT_FOR_PIPELINE_1_COMPLETION_PRECONDITION',
        external_dag_id='pipeline1_scrape_web_data', 
        external_task_id='DAG_RUN_COMPLETE', 
        # execution_date_fn=get_formatted_execution_date,
        execution_delta = timedelta(hours=12),
        timeout=TIMEOUT_IN_12_HOURS, 
        poke_interval=POKE_INTERVAL_10_MINS,
        dag=dag,
        allowed_states=['success', 'failed', 'skipped'],
        mode='poke'
    )
    mean_age_aggregated_by_occupation = SparkSubmitOperator(
        task_id='GENERATE_MEAN_AGE_AGGREGATED_BY_OCCUPATION_DATA',
        application='/app/generate_mean_age_aggregated_by_occupation.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '512m',
            'spark.executor.cores': '1',
            'spark.python.worker.memory': '512m',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.pyspark.python': 'python3.8',
            'spark.pyspark.driver.python': 'python3.8',
            # 'spark.master': 'spark://spark:7077'
            'spark.master': 'local[*]',
        },
        verbose=True,
        do_xcom_push=True,
        dag=dag
    )
    highest_rated_movies = SparkSubmitOperator(
        task_id='GENERATE_HIGHEST_RATES_MOVIES_DATA',
        application='/app/generate_top_20_highest_rated_movies.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '512m',
            'spark.executor.cores': '1',
            'spark.python.worker.memory': '512m',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.pyspark.python': 'python3.8',
            'spark.pyspark.driver.python': 'python3.8',
            # 'spark.master': 'spark://spark:7077'
            'spark.master': 'local[*]',
        },
        verbose=True,
        do_xcom_push=True,
        dag=dag
    ) 
    
    top_rated_genres_aggregated_by_user = SparkSubmitOperator(
        task_id='GENERATE_TOP_GENRS_AGGREGATED_BY_USER_AGE_GROUP',
        application='/app/generate_top_genres_by_age_group.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '512m',
            'spark.executor.cores': '1',
            'spark.python.worker.memory': '512m',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.pyspark.python': 'python3.8',
            'spark.pyspark.driver.python': 'python3.8',
            # 'spark.master': 'spark://spark:7077'
            'spark.master': 'local[*]',
        },
        verbose=True,
        do_xcom_push=True,
        dag=dag
    ) 
    
    similar_movies_data = SparkSubmitOperator(
        task_id='GENERATE_SIMILAR_MOVIES_DATA',
        application='/app/get_similar_10_movies.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '512m',
            'spark.executor.cores': '1',
            'spark.python.worker.memory': '512m',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.pyspark.python': 'python3.8',
            'spark.pyspark.driver.python': 'python3.8',
            # 'spark.master': 'spark://spark:7077'
            'spark.master': 'local[*]',
        },
        verbose=True,
        do_xcom_push=True,
        dag=dag
    ) 
    complete_dag = DummyOperator(task_id='DAG_RUN_COMPLETE', dag=dag)


    (
        wait_for_pipeline_1_completion_precondtion 
        >> mean_age_aggregated_by_occupation 
        >> highest_rated_movies 
        >> top_rated_genres_aggregated_by_user
        >> similar_movies_data
        >>  complete_dag
    )
