import io
import random
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from IPython.display import display, HTML

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
    """Convert DataFrame to Parquet format and return buffer."""
    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)
    return buffer