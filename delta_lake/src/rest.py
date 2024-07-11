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
# delta_config = {
#     "logStore": "s3",
#     "logStore.S3.endpoint": minio_endpoint,
#     "logStore.S3.accessKey": minio_access_key,
#     "logStore.S3.secretKey": minio_secret_key,
#     "logStore.S3.path": delta_log_path,
# }
# delta_config ={

#     "AWS_ENDPOINT_URL": minio_endpoint,
#     "AWS_REGION": "local",
#     "AWS_ACCESS_KEY_ID": minio_access_key,
#     "AWS_SECRET_ACCESS_KEY": minio_secret_key,
#     "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
#     "AWS_ALLOW_HTTP": "true"

# }

# delta_config ={

#     "endpoint": minio_endpoint,
#     "region": "local",
#     "key": minio_access_key,
#     "secret": minio_secret_key

# }
   
# Serialize the dictionary into a JSON string
# storage_options_str = json.dumps(storage_options)