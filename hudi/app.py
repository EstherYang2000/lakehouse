from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Hudi Example with MinIO") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .getOrCreate()

# MinIO configurations
minio_endpoint = "http://minio_hudi:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "scpm"

# Set MinIO credentials for Spark to access data
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)

# Example DataFrame
data = [
    ("1", "Alice", 34, "2023-01-01"),
    ("2", "Bob", 45, "2023-01-01"),
]
columns = ["uuid", "name", "age", "ts"]

df = spark.createDataFrame(data, ['id', 'ts', 'partitionpath'])

# Hudi table path (s3a:// protocol for MinIO)
hudi_table_path = f"localhost:9000//{minio_bucket}/hudi_table"  

# Hudi options for table creation and data ingestion
hudi_options = {
    "hoodie.table.name": "hudi_table",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "ts",
    "hoodie.datasource.write.table.name": "hudi_table",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.upsert.shuffle.parallelism": 2,
    "hoodie.insert.shuffle.parallelism": 2,
}

# Write DataFrame to Hudi table

df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_table_path)

# Stop SparkSession
spark.stop()
