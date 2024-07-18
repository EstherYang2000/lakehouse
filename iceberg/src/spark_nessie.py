import pyspark
from pyspark.sql import SparkSession
import os

## DEFINE SENSITIVE VARIABLES
NESSIE_URI = "http://localhost:19120/api/v1"
MINIO_ACCESS_KEY = "esther"
MINIO_SECRET_KEY = "esther"



conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-aws:1.5.2')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://iceberg')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://localhost:9000')
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
        .set('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000')  # Optional, specify your S3 endpoint
        .set('spark.hadoop.fs.s3a.path.style.access', 'true')  # Optional, configure path style access for custom endpoints
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')  # Ensure S3A filesystem is used
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')  # Ensure credentials provider is set
        .set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')  # Optional, disable SSL if using custom endpoint
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

## Test Run a Query
spark.sql("CREATE TABLE IF NOT EXISTS nessie.test1 (name string) USING iceberg;").show()
spark.sql("INSERT INTO nessie.test1 VALUES ('test');").show()
spark.sql("SELECT * FROM nessie.test1;").show()

# ## LOAD A CSV INTO AN SQL VIEW
# csv_df = spark.read.format("csv").option("header", "true").load("../datasets/df_open_2023.csv")
# csv_df.createOrReplaceTempView("csv_open_2023")

# ## CREATE AN ICEBERG TABLE FROM THE SQL VIEW
# spark.sql("CREATE TABLE IF NOT EXISTS nessie.df_open_2023 USING iceberg AS SELECT * FROM csv_open_2023;").show()

# ## QUERY THE ICEBERG TABLE
# spark.sql("SELECT * FROM nessie.df_open_2023 limit 10;").show()