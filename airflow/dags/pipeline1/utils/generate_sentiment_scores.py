import pandas
import logging
import json
from pipeline1.utils.mock_sentiment_api import mock_sentiment_api
logger = logging.getLogger(__name__)

def generate_sentiment_scores(**kwargs):
    task_instance = kwargs['task_instance']
    news_data = task_instance.xcom_pull(task_ids='CLEAN_AND_DEDUPLICATE_FETCHED_DATA', key='parsed_clean_deduplicated_data')
    
    if not news_data:
        logger.error("No data retrieved from previous task")
        return
    
    df = pandas.DataFrame(news_data)
    
    # Generate sentiment scores
    df_with_sentiment_score = df.assign(sentiment_score=lambda x: x.apply(lambda row: mock_sentiment_api(row['title'] + ' ' + row['article']), axis=1))
    
    task_instance.xcom_push(key='records_with_sentiments', value= json.loads(df_with_sentiment_score.to_json(orient='records')))
