from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def main():
    spark = SparkSession.builder \
        .appName("MeanAgeByOccupation") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.python.worker.memory", "512m") \
        .master("local[*]") \
        .getOrCreate()
  
    user_df = spark.read.csv(
        "/data/u.user", 
        sep="|",
        header=False,
        schema="user_id INT, age INT, gender STRING, occupation STRING, zip_code STRING"
    )

    result = user_df.groupBy("occupation").agg(mean("age").alias("mean_age")).orderBy("occupation")

    print("Mean Age by Occupation:")
    result.show(truncate=False)

    result_list = [row.asDict() for row in result.collect()]

    spark.stop()

    print("Creating postgres hook")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_name = 'occupation_mean_age'
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        occupation TEXT UNIQUE,
        mean_age FLOAT
    )
    """
    pg_hook.run(create_table_query)
    print(f"Created postgres table: {table_name}")

    print(f"Records to be inserted in postgres table: {result_list}")

    upsert_query = f"""
    INSERT INTO {table_name} (occupation, mean_age)
    VALUES (%s, %s)
    ON CONFLICT (occupation) 
    DO UPDATE SET mean_age = EXCLUDED.mean_age;
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for row in result_list:
                cur.execute(upsert_query, (row['occupation'], row['mean_age']))

    print(f"Upserted {len(result_list)} rows into {table_name}")
    
    verification_query = f"SELECT * FROM {table_name} LIMIT 5"
    verification_result = pg_hook.get_records(verification_query)
    print(f"First 5 rows after insertion: {verification_result}")
    
    return json.dumps(result_list)

if __name__ == "__main__":
    result = main()
    print(result)