import json
import pandas
import logging

logger = logging.getLogger(__name__)

def clean_and_deduplicate_data(**kwargs): 
    task_instance = kwargs['task_instance']
    parsed_data = task_instance.xcom_pull(key='parsed_data', task_ids='FETCH_DATA_FROM_WEB_SOURCES')
    if not parsed_data:
        logger.error("No data pulled from XCom")
        return
    dataframe = pandas.DataFrame(parsed_data) 
    logger.info(f"DataFrame columns: {dataframe.columns.tolist()}")
    logger.info(f"DataFrame shape: {dataframe.shape}")
    # Remove rows with bad inputs/missing title and desc
    clean_dataframe= dataframe.dropna(subset=['title', 'short_desc', 'post_url']) 

    # Deduplicate based on 'title' and 'short_desc'
    deduplicated_dataframe = clean_dataframe.drop_duplicates(subset=['title', 'short_desc'], keep='first')

    # Sort data by created_at for consistency 
    deduplicated_dataframe = deduplicated_dataframe.sort_values(by='created_at')

    task_instance.xcom_push(key='parsed_clean_deduplicated_data', value=json.loads(deduplicated_dataframe.to_json(orient='records')))