from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .getOrCreate()

# Path to your Parquet file
parquet_file_path = "data/mor_table/part-00000-b5ed11de-75cb-4bf8-aab5-abf16908673f-c000.snappy.parquet"

# Read Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)

# Show the content of the DataFrame
df.show()

# Optionally, you can perform further operations on 'df' as needed
# For example:
# df.printSchema()  # Print the schema of the DataFrame
# df.describe().show()  # Show summary statistics
# df.select("column_name").show()  # Select and show specific columns
# ...

# Stop the Spark session (only when you're done)
spark.stop()
