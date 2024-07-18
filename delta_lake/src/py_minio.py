from pyspark.sql import SparkSession
# Define sensitive MinIO credentials
MINIO_ACCESS_KEY = "esther"
MINIO_SECRET_KEY = "estheresther"

# MinIO endpoint and Delta Lake table path
MINIO_ENDPOINT = "http://localhost:9000"
bucket = "deltalake"
table = "cow_table"
DELTA_TABLE_PATH = f"s3a://{bucket}/{table}"

# Spark session configuration
spark = SparkSession.builder \
    .appName("Delta Lakes") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()


# spark = SparkSession.builder \
#     .appName("Hudi with MinIO") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .config("spark.sql.hive.convertMetastoreParquet", "false") \
#     .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
#     .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
#     .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4") \
#     .config("spark.jars.excludes", "org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-mapreduce-client-core") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#     .getOrCreate()
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
