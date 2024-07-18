from pyspark.sql import SparkSession

# Define sensitive MinIO credentials
MINIO_ACCESS_KEY = "your_minio_access_key"
MINIO_SECRET_KEY = "your_minio_secret_key"

# MinIO endpoint and Delta Lake table path
MINIO_ENDPOINT = "http://localhost:9000"
DELTA_TABLE_PATH = "s3a://your-bucket-name/delta-tables/my_delta_table"

# Spark session configuration
spark = SparkSession.builder \
    .appName("Delta Lake with MinIO Example") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()

try:
    # Create or use Delta Lake table
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{DELTA_TABLE_PATH}` (name STRING, age INT) USING delta")

    # Insert data into Delta Lake table
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.write.mode("append").format("delta").save(DELTA_TABLE_PATH)

    # Query Delta Lake table
    df_loaded = spark.read.format("delta").load(DELTA_TABLE_PATH)
    df_loaded.show()

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Stop Spark session
    spark.stop()
