from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Define local directory path
local_base_path = "./"

# Initialize SparkSession with Hudi and Hadoop AWS packages
spark = SparkSession.builder \
    .appName("Hudi with Local File System") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .getOrCreate()

# Configure Hudi options
hudi_options = {
    'hoodie.table.name': 'test_table',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': '2',
    'hoodie.insert.shuffle.parallelism': '2',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.table': 'test_table',
    'hoodie.datasource.hive_sync.partition_fields': 'partitionpath',
    'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://localhost:10000',
    'hoodie.datasource.hive_sync.username': 'hive',
    'hoodie.datasource.hive_sync.password': 'hive',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:9083'
}

# Create initial DataFrame
# Initialize a DataFrame (df) with some data
data = [("1", "New York"), ("2", "San Francisco")]
df = spark.createDataFrame(data, ["uuid", "city"])

# Write to Hudi table on local file system
table_path = local_base_path + "test_table"
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)

# Read Hudi table
hudi_df = spark.read.format("hudi").load(table_path)
hudi_df.show()

# Update DataFrame
update_data = [
    (1, '2021-01-01', 'partition1', 'new_value'),
    (2, '2021-01-02', 'partition1', 'new_value')
]

update_df = spark.createDataFrame(update_data, ['id', 'ts', 'partitionpath', 'new_field'])
update_df = update_df.withColumn('ts', lit('2021-01-05'))

# Upsert operation
update_df.write.format("hudi").options(**hudi_options).mode("append").save(local_base_path + "my_hudi_table")

# Read updated Hudi table
hudi_df_updated = spark.read.format("hudi").load(local_base_path + "my_hudi_table/*")
hudi_df_updated.show()

# Stop Spark session
spark.stop()
