from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskMarker
from airflow.operators.python import PythonOperator
from pipeline1.utils.fetch_api_and_parse_web_data import fetch_backend_api_get_parsed_data
from pipeline1.utils.clean_and_deduplicate_data import clean_and_deduplicate_data
from pipeline1.utils.generate_sentiment_scores import generate_sentiment_scores
from pipeline1.utils.persist_records_in_postgres import persist_records_in_postgres
from custom_schedule.working_day_schedule_timetable import WorkingDayScheduleTimeTable
from datetime import datetime


default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

working_day_timetable = WorkingDayScheduleTimeTable(13, 30) #7P.M IST CONVERTED IN UTC 1:30 PM


with DAG(
    dag_id='pipeline1_scrape_web_data', 
    default_args=default_args,
    schedule_interval=None,
    timetable=working_day_timetable) as dag:
    fecth_web_data = PythonOperator(
        task_id='FETCH_DATA_FROM_WEB_SOURCES',
        python_callable=fetch_backend_api_get_parsed_data,
        provide_context=True,
        dag=dag
    )

    clean_and_deduplicate_fetched_data =  PythonOperator(
        task_id='CLEAN_AND_DEDUPLICATE_FETCHED_DATA',
        python_callable=clean_and_deduplicate_data,
        provide_context=True,
        dag=dag
    )
    generated_sentiment_score = PythonOperator(
        task_id='GENERATE_SENTIMENT_SCORE_FOR_PROCESSED_DATA',
        python_callable=generate_sentiment_scores,
        provide_context=True,
        dag=dag
    )

    persist_processed_data = PythonOperator(
        task_id='PERSIST_PROCESS_DATA_IN_POSTGRES_DB',
        python_callable=persist_records_in_postgres,
        provide_context=True,
        dag=dag
    )

    complete_dag = ExternalTaskMarker(task_id='DAG_RUN_COMPLETE', 
                                      external_dag_id="pipeline2_analyse_ml_100k_data",
                                      external_task_id="WAIT_FOR_PIPELINE_1_COMPLETION_PRECONDITION",
                                      execution_date="{{ execution_date.replace(hour=13, minute=30).isoformat() }}"
                                    )
    

    (
        fecth_web_data 
        >> clean_and_deduplicate_fetched_data 
        >> generated_sentiment_score 
        >> persist_processed_data
        >>  complete_dag
    )
