from lakehouse.src.minio_utils import Minio
from minio.error import S3Error


def create_minio_client(endpoint, access_key, secret_key, secure=False):
    """創建 MinIO 客戶端並返回"""
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def ensure_bucket_exists(client, bucket_name):
    """確保 MinIO 存儲桶存在，若不存在則創建"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def upload_to_minio(client, bucket_name, file_name, buffer):
    """上傳緩衝區中的 Parquet 文件到 MinIO"""
    try:
        client.put_object(
            bucket_name,
            file_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        print(f"'{file_name}' is successfully uploaded to bucket '{bucket_name}'.")
    except S3Error as e:
        print(f"Error occurred: {e}")

def main():
    # MinIO 配置
    minio_endpoint = "your-minio-endpoint:9000"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    bucket_name = "your-bucket-name"
    parquet_file_name = "supply_chain_data.parquet"
    
    # 創建 MinIO 客戶端
    minio_client = create_minio_client(minio_endpoint, access_key, secret_key)
    
    # 確保存儲桶存在
    ensure_bucket_exists(minio_client, bucket_name)
    
    # 生成假供應鏈數據
    df = generate_fake_supply_chain_data(100)
    
    # 將 DataFrame 轉換為 Parquet 格式並寫入緩衝區
    parquet_buffer = dataframe_to_parquet_buffer(df)
    
    # 上傳 Parquet 文件到 MinIO
    upload_to_minio(minio_client, bucket_name, parquet_file_name, parquet_buffer)

if __name__ == "__main__":
    main()
