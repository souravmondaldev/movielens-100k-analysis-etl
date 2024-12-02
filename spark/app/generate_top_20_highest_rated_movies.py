from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def main():
    spark = SparkSession.builder \
        .appName("Top20HighestRatedMovies") \
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

    movie_stats = ratings_df.groupBy("movie_id") \
        .agg(
            avg("rating").alias("avg_rating"),
            count("rating").alias("rating_count")
        )

    top_movies = movie_stats.filter(movie_stats.rating_count >= 35) \
        .orderBy(desc("avg_rating"), desc("rating_count")) \
        .limit(20)

    movies_df = spark.read.csv(
        "/data/u.item", 
        sep="|",
        header=False,
        schema="movie_id INT, title STRING"
    ).select("movie_id", "title")

    result = top_movies.join(movies_df, "movie_id") \
        .select("title", "movie_id", "avg_rating", "rating_count") \
        .orderBy(desc("avg_rating"), desc("rating_count"))

    print("Top 20 Highest Rated Movies (minimum 35 ratings):")
    result.show(truncate=False)

    result_list = [row.asDict() for row in result.collect()]

    spark.stop()

    print("Creating postgres hook")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_name = 'top_20_highest_rated_movies'
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        movie_id INT UNIQUE,
        title TEXT,
        rating_count INT,
        avg_rating FLOAT
    )
    """
    pg_hook.run(create_table_query)
    print(f"Created postgres table: {table_name}")

    
    upsert_query = f"""
        INSERT INTO {table_name} (movie_id, title, rating_count, avg_rating)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (movie_id) 
        DO UPDATE SET 
            title = EXCLUDED.title,
            rating_count = EXCLUDED.rating_count,
            avg_rating = EXCLUDED.avg_rating
        """

    for row in result_list:
        pg_hook.run(upsert_query, parameters=(
            row['movie_id'],
            row['title'],
            row['rating_count'],
            row['avg_rating']
        ))

    print(f"Inserted {len(result_list)} rows into {table_name}")
    
    # Verify the insertion
    verification_query = f"SELECT * FROM {table_name} LIMIT 5"
    verification_result = pg_hook.get_records(verification_query)
    print(f"First 5 rows after insertion: {verification_result}")
    
    return json.dumps(result_list)

if __name__ == "__main__":
    result = main()
    print(result)