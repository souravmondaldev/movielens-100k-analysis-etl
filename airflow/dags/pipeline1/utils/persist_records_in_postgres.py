import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

logger = logging.getLogger(__name__)

def persist_records_in_postgres(**kwargs):
    task_instance = kwargs['task_instance']
    
    # Pull the data from the previous task
    data = task_instance.xcom_pull(task_ids='GENERATE_SENTIMENT_SCORE_FOR_PROCESSED_DATA', key='records_with_sentiments')
    
    if not data:
        logger.error("No data retrieved from previous task")
        return
    
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.error("DataFrame is empty after conversion")
        return
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    print("Creating postgres hook")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_name = 'news_sentiments'
    
    # Create table if not exists
    columns = ', '.join([f"{col} TEXT" for col in df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {columns}
    )
    """
    pg_hook.run(create_table_query)
    print(f"Created postgres table: {table_name}")

    
    rows = df.to_dict('records')
    print(f"Records to be insrted in  postgres table: {rows}")
    pg_hook.insert_rows(table_name, list(df.itertuples(index=False)), target_fields=df.columns.tolist())
    print(f"Inserted {len(rows)} rows into {table_name}")
    
    # Verify the insertion
    verification_query = f"SELECT * FROM {table_name} LIMIT 5"
    result = pg_hook.get_records(verification_query)
    print(f"First 5 rows after insertion: {result}")
    