import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Define sensitive MinIO credentials
MINIO_ACCESS_KEY = "esther"
MINIO_SECRET_KEY = "estheresther"

# MinIO endpoint and Delta Lake table path
MINIO_ENDPOINT = "http://localhost:9000"
bucket = "deltalake"
table = "cow_table"
# DELTA_TABLE_PATH = f"s3a://{bucket}/{table}"
DELTA_TABLE_PATH = f"data/{table}"
#  Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
# Create spark context
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Create spark context
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")
#  .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
#     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
#     .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-core_2.12:2.1.0") \
# Create a spark dataframe and write as a delta table
print("Starting Delta table creation")

# Create a spark dataframe and write as a delta table
# data = [(1, "Baratheon"),
#         (2, "Stark"),
#         (3, "Lannister")
#         ]
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
# ])
# sample_dataframe = spark.createDataFrame(data=data, schema=schema)
# sample_dataframe.repartition(1).write.mode(saveMode="append").format("delta").save(DELTA_TABLE_PATH)


# Update data in Delta
# print("Update data...!")
# # delta table path
# deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
# deltaTable.toDF().show()
# deltaTable.update(
#     condition=expr("id == 1"),
#     set={"name": lit("Esther")})
# deltaTable.toDF().show()


# Upsert Data
# print("Upserting Data...!")
# # delta table path
# deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
# deltaTable.toDF().show()
# # define new data
# data = [(2, "Andy"),
#         (4,"Jon"),
#         (5, "Ken")
#         ]
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True)
# ])
# newData = spark.createDataFrame(data=data, schema=schema)

# deltaTable.alias("oldData") \
#     .merge(
#     newData.alias("newData"),
#     "oldData.id = newData.id") \
#     .whenMatchedUpdate(
#     set={"name": col("newData.name")}) \
#     .whenNotMatchedInsert(values={"id": col("newData.id"), "name": col("newData.name")}) \
#     .execute()
# deltaTable.toDF().show()


# Delete Data
# print("Deleting data...!")
# # delta table path
# deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
# deltaTable.toDF().show()
# deltaTable.delete(condition=expr("name == 'Jon'"))
# deltaTable.toDF().show()

# Reading Older version of Data
# print("Read old data...!")

# df_versionzero = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_TABLE_PATH)
# df_versionzero.show()

# df_versionzone = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_TABLE_PATH)
# df_versionzone.show()

# Read data as of the specific timestamp
# Get the current timestamp after appending new data
# current_timestamp = datetime.now()
# timestamp_20_minutes_before = current_timestamp - timedelta(minutes=20)
# append_timestamp =timestamp_20_minutes_before.isoformat()
# print(f"Reading delta table as of {append_timestamp}:")
# df_as_of_timestamp = spark.read.format("delta").option("timestampAsOf", append_timestamp).load(DELTA_TABLE_PATH)
# df_as_of_timestamp.show()

# Schema Evolution https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/

# df = spark.read.format("delta").load(DELTA_TABLE_PATH)
# # df.write.option("overwriteSchema", "true")
# # Define new data with an additional column 'age'
# new_data = [(2, "Andy", 30), (4, "Jon", 25), (5, "Ken", 40)]
# new_schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True)  # New column 'age'
# ])
# new_df = spark.createDataFrame(data=new_data, schema=new_schema)
# # Append new data with schema evolution
# new_df.write \
#     .format("delta") \
#     .mode("append") \
#     .option("mergeSchema", "true") \
#     .save(DELTA_TABLE_PATH)
   
# Read Data
# print("Reading delta file ...!")
# df_versionzero = spark.read.format("delta").option("versionAsOf", 5).load(DELTA_TABLE_PATH)
# df_versionzero.show()
# got_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
# got_df.show() 


# Define new data with an additional column 'age'
# new_data = [(2, "Andy", 30,3000), (4, "Jon", 25,50000), (5, "Ken", 40,20000)]
# new_schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True),
#     StructField("salary", IntegerType(), True)# New column 'age'
# ])
# new_df = spark.createDataFrame(data=new_data, schema=new_schema)
# deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
# deltaTable.toDF().show()
# # Perform merge operation with schema evolution
# print("Upserting new data with schema evolution:")
# deltaTable.alias("oldData") \
#     .merge(
#     new_df.alias("newData"),
#     "oldData.id = newData.id") \
#     .whenMatchedUpdate(set={
#         "name": col("newData.name"),
#         "age": col("newData.age"),
#         "salary": col("newData.salary")
#     }) \
#     .whenNotMatchedInsert(values={
#         "id": col("newData.id"),
#         "name": col("newData.name"),
#         "age": col("newData.age"),
#         "salary": col("newData.salary")
#     }) \
#     .execute()

# Read and show data after schema evolution
# print("Data in delta table after schema evolution:")
# deltaTable = spark.read.format("delta").load(DELTA_TABLE_PATH)
# deltaTable.toDF().show()




# Define a function to convert string to datetime
def to_datetime(ts):
    return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

# # Define a function to get the history of the delta table
def get_versions_in_time_range(delta_table_path, start_time, end_time):
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    history_df = delta_table.history()
    print(history_df.show())
    start_time_dt = to_datetime(start_time)
    end_time_dt = to_datetime(end_time)
    versions_in_range = history_df.filter(
        (col("timestamp") >= start_time_dt) & 
        (col("timestamp") <= end_time_dt)
    ).select("version").collect()
    print(versions_in_range)
    
    return [row.version for row in versions_in_range]

# # Define the time range
start_time = "2024-07-19 00:05:00"
end_time = "2024-07-19 00:10:10"

# # Get versions within the time range
versions = get_versions_in_time_range(DELTA_TABLE_PATH, start_time, end_time)
# Read data from each version within the time range
dataframes = []
for version in versions:
    df_version = spark.read.format("delta").option("versionAsOf", version).load(DELTA_TABLE_PATH)
    dataframes.append(df_version)

# Union all dataframes to get the combined result
if dataframes:
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)
    combined_df.show()
else:
    print("No versions found in the specified time range.")