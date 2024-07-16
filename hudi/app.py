from pyspark.sql import SparkSession
# pyspark 3.4.0
# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "scpm"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Hudi with MinIO") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.jars.excludes", "org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-mapreduce-client-core") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()


# Path to the Parquet file in MinIO
table_name = "test_cow_table_minio"
partition_name = "san_francisco"
file_name = "4ab80b17-0b8d-4954-bd70-f35d03eee130-0_0-22-70_20240716153900160.parquet"
parquet_file_path = f"s3a://{minio_bucket}/{table_name}/{partition_name}/{file_name}"

# Read Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)

# Show the DataFrame schema
df.printSchema()

# Show sample data from the DataFrame
df.show()

# Stop SparkSession
spark.stop()
