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
from datetime import datetime

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--partition', type=str, default=False, help='Partition Data')
    parser.add_argument('--operation', type=str, choices=['append', 'overwrite', 'update','merge','read'], required=True, help='Operation to perform on the Delta table')

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
        
        elif args.operation == 'overwrite':
            overwrite_data_in_delta_table(spark_df, DELTA_TABLE_PATH)
        elif args.operation == 'merge':
            
            delta_table.alias("oldData") \
                .merge(
                spark_df.alias("newData"),
                "oldData.id = newData.id") \
                .whenMatchedUpdate(
                set={"name": col("newData.name")}) \
                .whenNotMatchedInsert(values={"id": col("newData.id"), "name": col("newData.name")}) \
                .execute()
                
        elif args.operation == 'read':
            # Read the existing Delta table
            existing_df = delta_table.toDF()
            print("Existing Delta Table Data:")
            existing_df.show()
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        delta_table.toDF().show()
    else:
        print("Delta table does not exist at the specified path.")

        # Write as a Delta table
        if args.partition:
            partition_columns = schema_yaml['partition']
            spark_df.repartition(1).write.mode("overwrite").format("delta").partitionBy(partition_columns).save(DELTA_TABLE_PATH)
        else:
            spark_df.repartition(1).write.mode("overwrite").format("delta").save(DELTA_TABLE_PATH)
        
        print("Data written to Delta table successfully.")
