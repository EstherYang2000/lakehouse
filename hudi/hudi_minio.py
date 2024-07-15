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
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-avro_2.12:3.4.0") \
    .config("spark.jars.excludes", "org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-mapreduce-client-core") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()


print(spark.version)
# pyspark
columns = ["ts","uuid","rider","driver","fare","city"]
data =[(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
       (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
       (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
       (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
       (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")]
inserts = spark.createDataFrame(data).toDF(*columns)

hudi_options = {
    'hoodie.table.name': "test_table",
    'hoodie.datasource.write.recordkey.field': 'ts',
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.insert.shuffle.parallelism': '2',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.table': 'test_table',
    'hoodie.datasource.hive_sync.partition_fields': 'partitionpath',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
    'hoodie.datasource.hive_sync.username': 'hive',
    'hoodie.datasource.hive_sync.password': 'hive',
    'hoodie.datasource.hive_sync.database': 'default',  # Add the database if necessary
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
}
basePath = f"s3a://{minio_bucket}/test_table"
inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)

# Configure Hudi options
# hudi_options = {
#     'hoodie.table.name': 'test_table',
#     'hoodie.datasource.write.recordkey.field': 'id',
#     'hoodie.datasource.write.partitionpath.field': 'partitionpath',
#     'hoodie.datasource.write.precombine.field': 'ts',
#     'hoodie.upsert.shuffle.parallelism': '2',
#     'hoodie.insert.shuffle.parallelism': '2',
#     'hoodie.datasource.hive_sync.enable': 'true',
#     'hoodie.datasource.hive_sync.table': 'test_table',
#     'hoodie.datasource.hive_sync.partition_fields': 'partitionpath',
#     'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
#     'hoodie.datasource.hive_sync.username': 'hive',
#     'hoodie.datasource.hive_sync.password': 'hive',
#     'hoodie.datasource.hive_sync.database': 'default',  # Add the database if necessary
#     'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
# }

# # Create initial DataFrame
# data = [
#     (1, '2021-01-01', 'partition1'),
#     (2, '2021-01-02', 'partition1'),
#     (3, '2021-01-03', 'partition2')
# ]

# df = spark.createDataFrame(data, ['id', 'ts', 'partitionpath'])
# table_path = f"s3a://{minio_bucket}/test_table"
# # Write the DataFrame to the Hoodie table
# df.write \
#   .format("hudi") \
#   .option("hoodie.datasource.write.recordkey.field", "id") \
#   .option("hoodie.datasource.write.partitionpath.field", "partitionpath") \
#   .option("hoodie.table.name", "test_table") \
#   .mode("overwrite") \
#   .save(table_path)
# # Read Hudi table
# hudi_df = spark.read.format("hudi").load(table_path)
# hudi_df.show()

# # Update DataFrame
# update_data = [
#     (1, '2021-01-01', 'partition1', 'new_value'),
#     (2, '2021-01-02', 'partition1', 'new_value')
# ]

# update_df = spark.createDataFrame(update_data, ['id', 'ts', 'partitionpath', 'new_field'])
# update_df = update_df.withColumn('ts', lit('2021-01-05'))
# # Upsert operation
# update_df.write.format("hudi").options(**hudi_options).mode("append").save(f"s3a://{minio_bucket}/my_hudi_table")

# # Read updated Hudi table
# hudi_df = spark.read.format("hudi").load(f"s3a://{minio_bucket}/my_hudi_table/*")
# hudi_df.show()

spark.stop()
