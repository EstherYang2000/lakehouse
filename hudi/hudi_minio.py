from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col  # 导入 col 函数
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

# pyspark 3.4.0
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
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.jars.excludes", "org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-mapreduce-client-core") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print(spark.version)


# 定義數據架構
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("uuid", StringType(), True),
    StructField("rider", StringType(), True),
    StructField("driver", StringType(), True),
    StructField("fare", FloatType(), True),
    StructField("city", StringType(), True)
])


# DataFrame data
columns = ["ts","uuid","rider","driver","fare","city"]
data =[
    (1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
    (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")
]
# 1.創建 DataFrame
inserts = spark.createDataFrame(data, schema)

table = "test_cow_table_minio"
# Configure Hudi options with Hive sync disabled
hudi_options = {
    'hoodie.table.name': table,
    'hoodie.datasource.write.table.type':"COPY_ON_WRITE",
    'hoodie.datasource.write.recordkey.field': 'ts',
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.insert.shuffle.parallelism': '10',
    'hoodie.upsert.shuffle.parallelism': '10',
    'hoodie.delete.shuffle.parallelism': '10',
    'hoodie.datasource.hive_sync.enable': 'true',  # Disable Hive sync
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': table,
    'hoodie.datasource.write.hive_style_partitioning': 'false',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
    
}

basePath = f"s3a://{minio_bucket}/{table}"

# 1. insert data to hudi table
inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)


# 2. Upsert Data
# upsert_data = [
#     (1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 20.10, "san_francisco"),  # Update fare
#     (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 28.70, "san_francisco"),  # Update fare
#     (1695200000000, "new-uuid-1", "rider-Z", "driver-Y", 45.50, "new_york")  # New record
# ]
# upserts = spark.createDataFrame(upsert_data).toDF(*columns)
# upserts.write.format("hudi"). \
#     options(**hudi_options). \
#     mode("append"). \
#     save(basePath)

# 3.Update data
# Lets read data from target Hudi table, modify fare column for rider-D and update it.
# updatesDf = spark.read.format("hudi").load(basePath).filter("rider == 'rider-D'").withColumn("fare",col("fare")*10)

# updatesDf.write.format("hudi"). \
#   options(**hudi_options). \
#   mode("append"). \
#   save(basePath)

# 4. Delete Data
# delete_data = [
#     (1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 20.1, "san_francisco"),  # Match schema
#     (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco")   # Match schema
# ]
# deletes = spark.createDataFrame(delete_data).toDF(*columns)

# deletes = deletes.withColumn("_hoodie_is_deleted", lit(True))

# deletes.write.format("hudi"). \
#     options(**hudi_options). \
#     mode("append"). \
#     option("hoodie.datasource.write.operation", "delete") \
#     .save(basePath)

# Read Hudi table
hudi_df = spark.read.format("hudi").load(basePath)
hudi_df.show()

# To enable Hive sync, ensure Hive Metastore is running and configured correctly.
# Uncomment and configure the following options if Hive sync is needed.
"""
hudi_options = {
    'hoodie.table.name': "test_table",
    'hoodie.datasource.write.recordkey.field': 'ts',
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.insert.shuffle.parallelism': '2',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.table': 'test_table',
    'hoodie.datasource.hive_sync.partition_fields': 'city',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
    'hoodie.datasource.hive_sync.username': 'hive',
    'hoodie.datasource.hive_sync.password': 'hive',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
}
"""

spark.stop()
