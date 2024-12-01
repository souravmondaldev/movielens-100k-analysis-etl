from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkJob") \
    .getOrCreate()

# Read 'u.user' file (ML-100K dataset)
data = spark.read.csv(
    "/app/static_data/u.user",
    sep="|",  # Use '|' as the delimiter for the MovieLens dataset
    schema="user_id INT, age INT, gender STRING, occupation STRING, zip_code STRING",
    header=False  # The u.user file does not have a header
)

# Print schema to verify
data.printSchema()

# Display data for verification (optional)
data.show()

# Write all rows to PostgreSQL (in this case, only 5 rows will be written)
data.limit(5).write.format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/airflow",
    driver="org.postgresql.Driver",
    dbtable="user_table",
    user="airflow",
    password="airflow"
).mode("overwrite").save()
