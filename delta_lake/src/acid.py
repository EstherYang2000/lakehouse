import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "scpm"

# Construct Delta Lake paths
delta_log_path = f"s3a://{minio_bucket}/deltalake_logs"
delta_table_path = f"s3a://{minio_bucket}/delta_table"

# Set Delta Lake configuration to use MinIO
# delta_config = {
#     "logStore": "s3",
#     "logStore.S3.endpoint": minio_endpoint,
#     "logStore.S3.accessKey": minio_access_key,
#     "logStore.S3.secretKey": minio_secret_key,
#     "logStore.S3.path": delta_log_path,
# }
# storage_options = {
#         "region": "us-east-1",
#         "endpoint_url": minio_endpoint,
#         "aws_access_key_id": minio_access_key,
#         "aws_secret_access_key": minio_secret_key,
#         "logStore.S3.path": delta_log_path,
# }
# Set Delta Lake configuration to use MinIO
delta_config = {
    "logStore": "s3",
    "region" : "",
    "logStore.S3.endpoint": minio_endpoint,
    "logStore.S3.accessKey": minio_access_key,
    "logStore.S3.secretKey": minio_secret_key,
    "logStore.S3.path": delta_log_path,
}
# Serialize the dictionary into a JSON string
# storage_options_str = json.dumps(storage_options)

# Create a Pandas DataFrame
df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

# Write DataFrame to Delta Lake table in MinIO
write_deltalake(delta_table_path, df,storage_options=delta_config)

# Append new data to existing Delta Lake table in MinIO
df_append = pd.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
write_deltalake(delta_table_path, df_append, mode="append",storage_options=delta_config)

# Read and display the Delta Lake table from MinIO
dt = DeltaTable(delta_table_path, storage_options=delta_config)
print(dt.to_pandas())
