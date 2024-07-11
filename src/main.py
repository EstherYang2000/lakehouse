import os
import io
import random
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

def generate_fake_supply_chain_data(num_records):
    """生成假供應鏈數據的 DataFrame"""
    data = {
        'order_id': [f'ORD{str(i).zfill(4)}' for i in range(1, num_records + 1)],
        'product_id': [f'PROD{str(i).zfill(4)}' for i in range(1, num_records + 1)],
        'product_name': [f'Product_{i}' for i in range(1, num_records + 1)],
        'supplier': [f'Supplier_{random.randint(1, 10)}' for _ in range(num_records)],
        'quantity': [random.randint(1, 100) for _ in range(num_records)],
        'unit_price': [round(random.uniform(10.0, 100.0), 2) for _ in range(num_records)],
        'order_date': [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(num_records)],
        'delivery_date': [datetime.now() + timedelta(days=random.randint(1, 30)) for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def dataframe_to_parquet_buffer(df):
    """將 DataFrame 轉換為 Parquet 格式並返回緩衝區"""
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)  # 重置緩衝區位置以便讀取
    return buffer
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
    minio_endpoint = "http://localhost:9000"
    access_key = "esther"
    secret_key = "estheresther"
    bucket_name = "scpm"
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