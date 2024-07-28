import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
import argparse
from delta_lake.src.delta_utils import yaml_to_spark_schema
from delta_lake.utils.read_data import read_file,read_yaml
import os
import pandas as pd
from datetime import datetime, timedelta
import time 
def convert_datetime_columns_to_string(df):
    # Convert all datetime64[ns] columns to datetime64[s]
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        # df[col] = df[col].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S%f'), '%Y-%m-%d %H:%M:%S')
        # df[col] = pd.to_datetime(df[col])
    return df

def append_data_to_delta_table(spark_df, path):
    spark_df.repartition(1).write.format("delta").mode("append").save(path)
    print("Data appended to Delta table successfully.")

def overwrite_data_in_delta_table(spark_df, path):
    spark_df.repartition(1).write.format("delta").mode("overwrite").save(path)
    print("Delta table overwritten successfully.")
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
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--partition', type=str, default=False, help='Partition Data')
    parser.add_argument('--operation', type=str, choices=['append','delete', 'overwrite', 'update','merge','schema_evolution','read','read_cdf'], help='Operation to perform on the Delta table')
    parser.add_argument('--read_mode', type=str, default='default',choices=['default','version','timestamp', 'time_travel'], help='Operation to perform on the Delta table')
    parser.add_argument('--deletion_vector', type=str, default=False, help='Deletion Vector')

    args = parser.parse_args()

    # Construct Delta Lake paths
    file_path = 'config/delta.yaml'  # Replace with your YAML file path
    yaml_data = read_yaml(file_path) 
    source_path = yaml_data["delta_table"]["delta_table_source_path"]
    DELTA_TABLE_PATH = os.path.join(yaml_data["delta_table"]["delta_table_path"])
    source_df = read_file(source_path)
    # Convert datetime columns to strings
    source_df = convert_datetime_columns_to_string(source_df)
    
    #  Create a spark session with Delta
    if args.deletion_vector:
        builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.changeDataFeed.enabled", "true") \
            .config("spark.databricks.delta.deletionVectors.enabled", "true")
    else:
        builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    # Create spark context
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Create a spark dataframe and write as a delta table
    schema_yaml = read_yaml('config/schema.yaml')
    spark_schema = yaml_to_spark_schema(schema_yaml['schema'])
    # Convert source DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(data=source_df.to_dict(orient='records'), schema=spark_schema)


    #Check if Delta table exists
    if DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
        print("Delta table exists at the specified path.")
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        delta_table.toDF().show()
        if args.operation == 'update':
            # update the Delta table with condition
            delta_table.update(
                condition=expr("supplier == 'Supplier_9'"),
                set={"unit_price": lit(22.63)})
        elif args.operation == 'append':
            append_data_to_delta_table(spark_df, DELTA_TABLE_PATH)
        
        elif args.operation == 'delete':
            print("Deleting data...!")
            deltaTable = DeltaTable.forPath(spark_df, DELTA_TABLE_PATH)
            deltaTable.toDF().show()
            deltaTable.delete(condition=expr("supplier == 'Supplier_4'"))
            deltaTable.toDF().show()
        elif args.operation == 'overwrite':
            #Start time calculation
            start_time = time.time()
            print(start_time)
            overwrite_data_in_delta_table(spark_df, DELTA_TABLE_PATH)
            # # End time calculation
            end_time = time.time()
            print(end_time)
            # Calculate the complete time
            complete_time = end_time - start_time
            print(f"Complete time taken: {complete_time} seconds")
        elif args.operation == 'merge':
            #Start time calculation
            start_time = time.time()
            print(start_time)
            # Assuming delta_table is your DeltaTable object
            delta_table.alias("target") \
                .merge(spark_df.alias("source"),"target.order_id = source.order_id") \
                .whenNotMatchedBySourceDelete() \
                .whenNotMatchedInsertAll() \
                .whenMatchedUpdateAll() \
                .execute()
            # # End time calculation
            end_time = time.time()
            print(end_time)
            # Calculate the complete time
            complete_time = end_time - start_time
            print(f"Complete time taken: {complete_time} seconds")
        elif args.operation == 'schema_evolution':
            # Define new data with an additional column 'age'
            new_data = [
                ("1", "prod_1", "Product 1", "Supplier_1", 10, 20.5, "2024-07-26 12:00:00", "2024-07-26 12:00:00",10),
                ("2", "prod_2", "Product 2", "Supplier_2", 15, 30.75, "2024-07-26 12:00:00", "2024-07-26 12:00:00",10),
                ("3", "prod_3", "Product 3", "Supplier_3", 20, 40.00, "2024-07-26 12:00:00", "2024-07-26 12:00:00",10),
                ("4", "prod_4", "Product 4", "Supplier_4", 25, 50.25, "2024-07-26 12:00:00", "2024-07-26 12:00:00",10)
            ]
            new_schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("supplier", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("order_date", StringType(), True),
                StructField("delivery_date", StringType(), True),
                StructField("age", IntegerType(), True)  # New column 'age'
            ])
            new_df = spark.createDataFrame(data=new_data, schema=new_schema)
            # Append new data with schema evolution
            new_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(DELTA_TABLE_PATH)
        elif args.operation == 'read':
            if args.read_mode =='default':
                # Read the existing Delta table
                existing_df = delta_table.toDF()
                print("Existing Delta Table Data:")
                existing_df.show()
            elif args.read_mode =='version':
                # Reading Older version of Data
                print("Read detla data with version...!")
                df_version = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_TABLE_PATH)
                df_version.show()
            elif args.read_mode =='timestamp':
                # Get the current timestamp after appending new data
                current_timestamp = datetime.now()
                timestamp_20_minutes_before_str = '2024-07-26 10:55:28'
                timestamp_20_minutes_before = datetime.strptime(timestamp_20_minutes_before_str, '%Y-%m-%d %H:%M:%S')
                # timestamp_20_minutes_before = current_timestamp - timedelta(minutes=120)
                append_timestamp = timestamp_20_minutes_before.isoformat()
                print(f"Reading delta table as of {append_timestamp}:")
                df_as_of_timestamp = spark.read.format("delta").option("timestampAsOf", append_timestamp).load(DELTA_TABLE_PATH)
                df_as_of_timestamp.show()
            elif args.read_mode =='time_travel':
                # # timestamps as formatted timestamp
                starting_timestamp = '2024-07-26 10:50:00'
                ending_timestamp = '2024-07-26 10:53:00'
                # print(f"Read delta table with time travel from {starting_timestamp} to {ending_timestamp}...")

                # version = spark.read.format("delta") \
                #         .option("readChangeFeed", "true") \
                #         .option("startingTimestamp", '2024-07-26 10:50:00') \
                #         .option("endingTimestamp", '2024-07-26 10:53:00') \
                #         .load(DELTA_TABLE_PATH)
                # print(version)
                # version_filtered = version.filter((col("_commit_timestamp") >= starting_timestamp) & (col("_commit_timestamp") <= ending_timestamp))
                # print(version_filtered)
                # version_filtered.show()
                # Get versions within the time range
                versions = get_versions_in_time_range(DELTA_TABLE_PATH, starting_timestamp, ending_timestamp)
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
        elif args.operation == 'read_cdf':
            print("Reading Change Data Feed...")
            cdf_df = spark.read.format("delta") \
                .option("readChangeData", "true") \
                .option("startingVersion", 3) \
                .option("endingVersion", 4) \
                .load(DELTA_TABLE_PATH)
            print("Change Data Feed:")
            print(cdf_df)
            cdf_df.show()
            # Filter for deletions
            deletions_df = cdf_df.filter(col("_change_type") == "delete")
            print("Deletions:")
            deletions_df.show()
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        history_df = delta_table.history()
        print(history_df.show())
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        delta_table.toDF().show()
    else:
        print("Delta table does not exist at the specified path.")

        # Write as a Delta table
        if args.partition:
            
            partition_columns = schema_yaml['partition']
            if args.deletion_vector:
                spark_df.repartition(1).write.mode("overwrite").format("delta").option("delta.enableDeletionVectors", "true").partitionBy(partition_columns).save(DELTA_TABLE_PATH)
            else:
                spark_df.repartition(1).write.mode("overwrite").format("delta").partitionBy(partition_columns).save(DELTA_TABLE_PATH)
        else:
            if args.deletion_vector:
                spark_df.repartition(1).write.mode("overwrite").format("delta").option("delta.enableDeletionVectors", "true").save(DELTA_TABLE_PATH)
            else:
                spark_df.repartition(1).write.mode("overwrite").format("delta").save(DELTA_TABLE_PATH)

        if args.deletion_vector:
            DELTA_TABLE_PATH = "file:////Users/yangyujie/Documents/tsmc/lakehouse/delta_lake/data/scmp-stage-layer/cpo/ba_dmnd_data_pyspark_deletion_vector"

            # Enable deletion vector
            delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
            # Enable deletion vectors on existing table
            spark.sql(f"ALTER TABLE delta.`{DELTA_TABLE_PATH}` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
            spark.sql(f"ALTER TABLE delta.`{DELTA_TABLE_PATH}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
            table_details = spark.sql(f"DESCRIBE DETAIL delta.`{DELTA_TABLE_PATH}`")
            table_details.show(truncate=False)
            print("Deletion vectors enabled on the existing Delta table.")
            
        print("Data written to Delta table successfully.")
