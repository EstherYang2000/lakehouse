from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "scpm"

# Initialize SparkSession with Hudi and Hadoop AWS packages
spark = SparkSession.builder \
    .appName("Hudi with MinIO") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Configure Hudi options
hudi_options = {
    'hoodie.table.name': 'my_hudi_table',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': '2',
    'hoodie.insert.shuffle.parallelism': '2',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.table': 'my_hudi_table',
    'hoodie.datasource.hive_sync.partition_fields': 'partitionpath',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
    'hoodie.datasource.hive_sync.username': 'hive',
    'hoodie.datasource.hive_sync.password': 'hive',
    'hoodie.datasource.hive_sync.database': 'default',  # Add the database if necessary
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
}

# Create initial DataFrame
data = [
    (1, '2021-01-01', 'partition1'),
    (2, '2021-01-02', 'partition1'),
    (3, '2021-01-03', 'partition2')
]

df = spark.createDataFrame(data, ['id', 'ts', 'partitionpath'])

# Write to Hudi table
df.write.format("hudi").options(**hudi_options).mode("append").save(f"s3a://{minio_bucket}/my_hudi_table")

# Read Hudi table
hudi_df = spark.read.format("hudi").load(f"s3a://{minio_bucket}/my_hudi_table/*")
hudi_df.show()

# Update DataFrame
update_data = [
    (1, '2021-01-01', 'partition1', 'new_value'),
    (2, '2021-01-02', 'partition1', 'new_value')
]

update_df = spark.createDataFrame(update_data, ['id', 'ts', 'partitionpath', 'new_field'])
update_df = update_df.withColumn('ts', lit('2021-01-05'))

# Upsert operation
update_df.write.format("hudi").options(**hudi_options).mode("append").save(f"s3a://{minio_bucket}/my_hudi_table")

# Read updated Hudi table
hudi_df = spark.read.format("hudi").load(f"s3a://{minio_bucket}/my_hudi_table/*")
hudi_df.show()

spark.stop()
