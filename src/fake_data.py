import io
import random
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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