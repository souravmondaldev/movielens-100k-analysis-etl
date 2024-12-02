from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, count, desc, col, when, explode, split, coalesce, concat_ws
from pyspark.sql.window import Window
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
  
    users_df = spark.read.csv(
        "/data/u.user",
        sep="|",
        header=False,
        schema="user_id INT, age INT, gender STRING, occupation STRING, zip_code STRING"
    )

    users_df = users_df.withColumn("age_group", 
        when(col("age") < 20, "Under 20")
        .when((col("age") >= 20) & (col("age") < 25), "20-24")
        .when((col("age") >= 25) & (col("age") < 35), "25-34")
        .when((col("age") >= 35) & (col("age") < 45), "35-44")
        .otherwise("45 and older")
    )

    ratings_df = spark.read.csv(
        "/data/u.data",
        sep="\t",
        header=False,
        schema="user_id INT, movie_id INT, rating FLOAT, timestamp LONG"
    )
    """
    SCHEMA FROM README: movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
    """
    movies_df = spark.read.csv(
        "/data/u.item",
        sep="|",
        header=False,
        schema="movie_id INT, title STRING, release_date STRING, video_release_date STRING, imdb_url STRING, unknown INT, Action INT, Adventure INT, Animation INT, Children INT, Comedy INT, Crime INT, Documentary INT, Drama INT, Fantasy INT, Film_Noir INT, Horror INT, Musical INT, Mystery INT, Romance INT, Sci_Fi INT, Thriller INT, War INT, Western INT"
    )

    genre_columns = movies_df.columns[5:]  
    movies_with_genres = movies_df.select(
        col("movie_id"),
        explode(split(concat_ws(",", *[when(col(c) == 1, c) for c in genre_columns]), ",")).alias("genre")
    )

    joined_df = ratings_df.join(movies_with_genres, "movie_id") \
                          .join(users_df, "user_id")

    genre_counts = joined_df.groupBy("occupation", "age_group", "genre") \
                            .agg(count("*").alias("rating_count")) \
                            .orderBy("occupation", "age_group", desc("rating_count"))

    window_spec = Window.partitionBy("occupation", "age_group").orderBy(desc("rating_count"))
    top_genres = genre_counts.withColumn("rank", rank().over(window_spec)) \
                             .filter(col("rank") == 1) \
                             .drop("rank")
                             

    print("Top Genres by Occupation and Age Group:")
    top_genres.show(truncate=False)
    top_genres.printSchema()
    result_list = [row.asDict() for row in top_genres.collect()]

    spark.stop()

    print("Creating postgres hook")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_name = 'top_genres_by_occupation_age_group'
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        occupation TEXT,
        age_group TEXT,
        genre TEXT
    )
    """
    pg_hook.run(create_table_query)
    print(f"Created postgres table: {table_name}")

    print(f"Records to be inserted in postgres table: {result_list}")
    rows_to_insert = [
        (row['occupation'], row['age_group'], row['genre'])
        for row in result_list
    ]

    insert_sql = f"""
    INSERT INTO {table_name} (occupation, age_group, genre)
    VALUES (%s, %s, %s)
    """

    # Use executemany for better performance with multiple inserts
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows_to_insert)
    
    print(f"Inserted {len(result_list)} rows into {table_name}")
    
    # Verify the insertion
    verification_query = f"SELECT * FROM {table_name} LIMIT 5"
    verification_result = pg_hook.get_records(verification_query)
    print(f"First 5 rows after insertion: {verification_result}")
    
    return json.dumps(result_list)

if __name__ == "__main__":
    result = main()
    print(result)