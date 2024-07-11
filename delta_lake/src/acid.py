import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
import os
# Define MinIO endpoint and credentials
minio_endpoint = "http://minio:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "scpm"

# Construct Delta Lake paths
delta_log_path = f"s3a://{minio_bucket}/deltalake_logs"
delta_table_path = f"s3a://{minio_bucket}/delta_table"

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



# Create a Pandas DataFrame
df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

# Write DataFrame to Delta Lake table in MinIO
write_deltalake(delta_table_path, df,storage_options=delta_config)

# Append new data to existing Delta Lake table in MinIO
# df_append = pd.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
# write_deltalake(delta_table_path, df_append, mode="overwrite",storage_options=delta_config)

# Read and display the Delta Lake table from MinIO
dt = DeltaTable(delta_table_path,storage_options=delta_config)
print(dt.to_pandas())
