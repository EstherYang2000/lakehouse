from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IcebergOperations") \
    .config("spark.sql.catalog.ic", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ic.type", "hive") \
    .config("spark.sql.catalog.ic.warehouse", "file://{warehouse}") \
    .getOrCreate()
# Read data into a PySpark DataFrame
df = spark.read.parquet("data/namelist.parquet")
# Import necessary PySpark SQL functions
from pyspark.sql.functions import col

# Define the initial schema
schema = "id INT, name STRING"

# Create Iceberg table
spark.sql(f"CREATE TABLE namelist1_dataset USING iceberg OPTIONS(path='iceberg.namelist1_dataset', schema='{schema}')")

# Append data to the Iceberg table
df.writeTo("iceberg_table").append()
