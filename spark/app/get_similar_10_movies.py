from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, avg, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def main():
    spark = SparkSession.builder \
        .appName("MovieSimilarityJob") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.python.worker.memory", "512m") \
        .master("local[*]") \
        .getOrCreate()

    ratings_df = spark.read.csv(
        "/data/u.data",
        sep="\t",
        header=False,
        schema="user_id INT, movie_id INT, rating FLOAT, timestamp LONG"
    )

    movie_12_ratings = ratings_df.filter(col("movie_id") == 12)

    joined_ratings = movie_12_ratings.alias("a").join(
        ratings_df.alias("b"),
        (col("a.user_id") == col("b.user_id")) & (col("a.movie_id") != col("b.movie_id"))
    )

    total_ratings_12 = movie_12_ratings.count()

    similarity_df = joined_ratings.groupBy("b.movie_id").agg(
        (count("*") / lit(total_ratings_12)).alias("co_occurrence"),
        (1 - avg(spark_abs(col("a.rating") - col("b.rating"))) / 4).alias("similarity")
    )


    similar_movies = similarity_df.filter(
        (col("similarity") >= 0.95) & (col("co_occurrence") >= 0.5)
    )

    window = Window.orderBy(col("similarity").desc(), col("co_occurrence").desc())
    top_10_similar = similar_movies.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 10) \
        .select("movie_id", "similarity", "co_occurrence")

    result_list = top_10_similar.collect()
    result_dicts = [row.asDict() for row in result_list]

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_name = 'similar_movies_to_movie_id_12'

    # Create table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        movie_id INT,
        similarity FLOAT,
        co_occurrence FLOAT
    )
    """
    pg_hook.run(create_table_query)

    pg_hook.insert_rows(table_name, 
                        [(d['movie_id'], d['similarity'], d['co_occurrence']) for d in result_dicts],
                        target_fields=['movie_id', 'similarity', 'co_occurrence'])

    print(f"Inserted {len(result_dicts)} rows into {table_name}")

    # Verify insertion
    verification_query = f"SELECT * FROM {table_name}"
    verification_result = pg_hook.get_records(verification_query)
    print(f"Rows in {table_name}: {verification_result}")

    spark.stop()
    return json.dumps(result_dicts)

if __name__ == "__main__":
    result = main()
    print(result)
