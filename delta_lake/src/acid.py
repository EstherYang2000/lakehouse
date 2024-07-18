import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyarrow as pa
from pyspark.sql.functions import * # https://docs.delta.io/latest/delta-update.html


# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "deltalake"
table = "cow_table"
# Construct Delta Lake paths
delta_log_path = f"s3a://{minio_bucket}/deltalake_logs"
delta_table_path = f"s3a://{minio_bucket}/{table}"

delta_config ={

    "AWS_ENDPOINT_URL": minio_endpoint,
    "AWS_REGION": "local",
    "AWS_ACCESS_KEY_ID": minio_access_key,
    "AWS_SECRET_ACCESS_KEY": minio_secret_key,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
    "AWS_EC2_METADATA_DISABLED": "true",
    "logStore": "s3",
    "logStore.S3.endpoint": minio_endpoint,
    "logStore.S3.accessKey": minio_access_key,
    "logStore.S3.secretKey": minio_secret_key,
    "logStore.S3.path": delta_log_path,

}
# os.environ['S3_ACCESS_KEY'] = minio_access_key
# os.environ['S3_SECRET_KEY'] = minio_secret_key
# os.environ['S3_ENDPOINT'] = minio_endpoint
# os.environ['S3_REGION'] = "local"
# os.environ['S3_PATH_STYLE_ACCESS'] = "true"

# Define schema for the Delta Lake table 
# schema = pa.schema([
#     ("id", pa.int32()),
#     ("name", pa.string())
# ])
# Create a Pandas DataFrame
# df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
# df = df.astype({"id": "int32", "name": "string"})  # Ensure Pandas dtype matches Spark schema

# Write DataFrame to Delta Lake table in MinIO
# write_deltalake(delta_table_path, df,schema=schema,storage_options=delta_config)

# # Append new data to existing Delta Lake table in MinIO append
# df_append = pd.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
# df_append = df_append.astype({"id": "int32", "name": "string"})
# write_deltalake(delta_table_path, df_append, mode="append",storage_options=delta_config)

# # Append new data to existing Delta Lake table in MinIO overwrite 
df_append = pd.DataFrame({"id": [4], "name": ["Esther"]})
df_append = df_append.astype({"id": "int32", "name": "string"})
write_deltalake(delta_table_path, df_append, mode="overwrite",storage_options=delta_config)


# # update data to existing Delta Lake table in MinIO overwrite  
# Load existing Delta Lake table into a Pandas DataFrame
# df_existing = DeltaTable(delta_table_path,storage_options=delta_config).to_pandas()
# Update or modify data in the DataFrame
# df_existing.loc[df_existing['id'] == 4, 'name'] = 'Esther'
# Ensure the DataFrame types match the expected schema
# df_existing = df_existing.astype({"id": "int32", "name": "string"})
# Write the updated DataFrame back to Delta Lake table in MinIO
# write_deltalake(delta_table_path, df_existing, mode="overwrite", storage_options=delta_config)

## Schema Evolution 
# df_append = pd.DataFrame({"id": [4, 5], "name": ["David", "Eve"],"price": [200, 300]})
# Update schema if necessary (ensure new schema includes all columns)
# updated_schema = pa.schema([
#     ("id", pa.int32()),
#     ("name", pa.string()),
#     ("price", pa.int32())
# ])


## Delete Data 
# df_existing = DeltaTable(delta_table_path,storage_options=delta_config)
# df_existing.delete("id = 5")




# Write new DataFrame with updated schema https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/
# write_deltalake(delta_table_path, df_append, schema=updated_schema, mode="append", storage_options=delta_config)




# write_deltalake('path/to/table', df, partition_by=['y'])


# Define update data
# df_update = pd.DataFrame({"id": [1, 2], "name": ["Updated Alice", "Updated Bob"]})
# # Perform merge operation to update records based on id column
# merge(delta_table_path, df_update, on="id", storage_options=delta_config)

#Read and display the Delta Lake table from MinIO
dt = DeltaTable(delta_table_path,storage_options=delta_config)
print(dt.to_pandas())

# Time Travel

# dt = DeltaTable("../rust/tests/data/simple_table", version=2)
# print(dt.to_pandas())
