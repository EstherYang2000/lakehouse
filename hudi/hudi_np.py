from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col  # 导入 col 函数
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType,IntegerType

# pyspark 3.4.0
# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "hudi"

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

table = "test_mor_np_table_minio"
# Configure Hudi options with Hive sync disabled
hudi_options = {
    'hoodie.table.name': table,
    'hoodie.datasource.write.table.type':"MERGE_ON_READ",
    'hoodie.datasource.write.recordkey.field': 'ts',
    'hoodie.insert.shuffle.parallelism': '10',
    'hoodie.upsert.shuffle.parallelism': '10',
    'hoodie.delete.shuffle.parallelism': '10',
    'hoodie.datasource.hive_sync.enable': 'false',  # Disable Hive sync
    'hoodie.datasource.write.hive_style_partitioning': 'false'
}

basePath = f"s3a://{minio_bucket}/{table}"

#1. insert data to hudi table
inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)


# 2. Upsert Data
upsert_data = [
    (1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 20.10, "san_francisco"),  # Update fare
    (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 28.70, "san_francisco"),  # Update fare
    (1695200000000, "new-uuid-1", "rider-Z", "driver-Y", 45.50, "new_york")  # New record
]
# # # Define updated schema for schema evolution
# updated_schema = StructType([
#     StructField("ts", LongType(), True),
#     StructField("uuid", StringType(), True),
#     StructField("rider", StringType(), True),
#     StructField("driver", StringType(), True),
#     StructField("fare", FloatType(), True),
#     StructField("city", StringType(), True)
#     # StructField("bonus_points", IntegerType(), True)  
# ])

# Update DataFrame with updated schema for schema evolution
# updated_data = [
#     (1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco", 100),
#     (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco", 150),
#     (1695200000000, "new-uuid-1", "rider-Z", "driver-Y", 45.50, "new_york", 200)  # New record with bonus_points
# ]

# # Create DataFrame with updated schema
# updated_df = spark.createDataFrame(upsert_data, updated_schema)

# # Upsert updated data with updated schema for schema evolution
# updated_df.write.format("hudi"). \
#     options(**hudi_options). \
#     mode("append"). \
#     save(basePath)

# upsert_wrong_type_data = [
#     (1695200000001, "new-uuid-2", "rider-Z", "driver-Y", 45.50, "new_york", "200")  # New record with bonus_points
# ]
# # upserts = spark.createDataFrame(upsert_wrong_type_data, updated_schema)
# columns = ["ts","uuid","rider","driver","fare","city","bonus_points"]
 
# upserts = spark.createDataFrame(upsert_wrong_type_data).toDF(*columns)
# upserts.write.format("hudi"). \
#     options(**hudi_options). \
#     mode("append"). \
#     save(basePath)
#3.Update data
#Lets read data from target Hudi table, modify fare column for rider-D and update it.
# updatesDf = spark.read.format("hudi").load(basePath).filter("rider == 'rider-D'").withColumn("fare",col("fare")*10)

# updatesDf.write.format("hudi"). \
#   options(**hudi_options). \
#   mode("append"). \
#   save(basePath)

# 4. Delete Data
# delete_data = [
#     (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco", 150)
# ]
# deletes = spark.createDataFrame(delete_data).toDF(*columns)

# deletes = deletes.withColumn("_hoodie_is_deleted", lit(True))

# deletes.write.format("hudi"). \
#     options(**hudi_options). \
#     mode("append"). \
#     option("hoodie.datasource.write.operation", "delete") \
#     .save(basePath)

# Read Hudi table
# commit_to_query = "20240717131956958"
# hudi_df = spark.read.format("hudi").load(basePath)
# hudi_df.show()
# hudi_df_at_commit = spark.read.format("hudi") \
#                       .option("hoodie.datasource.query.as.of", commit_to_query) \
#                       .load(basePath)
# hudi_df_at_commit.show()

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


### READ THE QUERY

# pyspark
# reload data
# spark.read.format("hudi").load(basePath).createOrReplaceTempView(table)

# commits = list(map(lambda row: row[0], spark.sql(f"SELECT DISTINCT(_hoodie_commit_time) AS commitTime FROM  {table} ORDER BY commitTime").limit(50).collect()))
# # beginTime = commits[len(commits) - 2] # commit time we are interested in
# beginTime = commits[0]
# print(commits)
# print(beginTime)
# # # incrementally query data
# incremental_read_options = {
#   'hoodie.datasource.query.type': 'incremental',
#   'hoodie.datasource.read.begin.instanttime': commits[0],
#   'hoodie.datasource.read.end.instanttime': commits[1]  # Optional: Set end instant time to first commit

# }
# tripsIncrementalDF = spark.read.format("hudi"). \
#   options(**incremental_read_options). \
#   load(basePath)
# tripsIncrementalDF.createOrReplaceTempView("trips_incremental")

# spark.sql("SELECT `_hoodie_commit_time`, city,fare, rider, driver, uuid, ts FROM trips_incremental").show()

# spark.stop()
