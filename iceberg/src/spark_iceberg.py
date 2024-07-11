from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("IcebergIntegration") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "glue") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/") \
    .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "esther") \
    .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "estheresther") \
    .config("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.region", "local") \
    .getOrCreate()

# Define Glue catalog
catalog = load_catalog(
    "glue_catalog",
    type="glue",
    warehouse="s3a://iceberg/",
    properties={
        "iceberg.catalog.type": "glue",
        "fs.s3a.endpoint": "http://minio:9000",
        "fs.s3a.access.key": "esther",
        "fs.s3a.secret.key": "estheresther",
        "fs.s3a.region_name": "true",
        "glue.region": "local",  # Specify your AWS region here for Glue
    },
)

# Create a table schema
schema = Schema(
    NestedField("id", IntegerType()),
    NestedField("name", StringType())
)

# Create a new Iceberg table
catalog.create_table("glue_catalog.db.my_table", schema)

# Insert data into the table
from pyiceberg.table import Table

table: Table = catalog.load_table("glue_catalog.db.my_table")

with table.new_append() as append:
    append.append_file({
        "id": 1,
        "name": "Alice"
    })
    append.append_file({
        "id": 2,
        "name": "Bob"
    })
    append.commit()

# Query the table using PyIceberg
rows = table.scan().select(["id", "name"]).collect()
for row in rows:
    print(row)

# Query the table using Spark SQL
df = spark.sql("SELECT * FROM glue_catalog.db.my_table")
df.show()