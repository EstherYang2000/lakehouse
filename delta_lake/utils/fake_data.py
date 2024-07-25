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
import numpy as np
def generate_fake_supply_chain_data(num_records):
    """生成假供應鏈數據的 DataFrame"""
    data = {
        # 'order_id': [f'ORD{str(i).zfill(4)}' for i in range(13, 13 + num_records)],
        'order_id': [f'ORD{str(i).zfill(4)}' for i in range(12, num_records+12)],
        'product_id': [f'PROD{str(i).zfill(4)}' for i in range(12, num_records+12)],
        # 'product_id': [f'PROD{str(i).zfill(4)}' for i in range(13, 13 + num_records)],
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

def randomly_update_columns(df):
    num_records = len(df)
    # Randomly select indices to update
    indices_to_update = random.sample(range(num_records), k=int(num_records * 0.3))  # Update 30% of records
    # Update selected columns with random values
    for idx in indices_to_update:
        df.at[idx, 'quantity'] = random.randint(1, 100)
        df.at[idx, 'unit_price'] = round(random.uniform(10.0, 100.0), 2)
        df.at[idx, 'order_date'] = datetime.now() - timedelta(days=random.randint(1, 30))
        df.at[idx, 'delivery_date'] = datetime.now() + timedelta(days=random.randint(1, 30))
    return df

    
def manipulate_data(df, mode):
    """Manipulate Pandas DataFrame based on mode ('update', 'upsert', 'delete', 'mixed')."""
    if mode == 'update':
        # Update existing keys with random values
        df_copy = df.copy()
        idx = int(np.random.randint(1, len(df_copy), 1)[0])
        print(idx)
        df_copy.at[idx, 'unit_price'] = round(random.uniform(10.0, 100.0), 2)
        return df_copy
        
    elif mode == 'upsert':
        # Add new entries with random values
        new_data = generate_fake_supply_chain_data(1)
        updated_df = manipulate_data(df, "update")
        return pd.concat([updated_df, new_data], ignore_index=True)
    elif mode == 'delete':
        # Remove a random entry
        if len(df) > 0:
            idx_to_delete = np.random.randint(0, len(df))
            print(idx_to_delete)
            return df.drop(idx_to_delete).reset_index(drop=True)
        else:
            return df.copy()
        
    elif mode == 'mixed':
        # Perform mixed operations (delete and upsert) with random probabilities
        df_copy = df.copy()
        # Delete a random entry
        if len(df_copy) > 0:
            deleted_df = manipulate_data(df_copy, "delete")
            mixed_df = manipulate_data(deleted_df, "upsert")
            return mixed_df
        else:

            return df_copy
        
    else:
        raise ValueError("Invalid mode. Choose from 'update', 'upsert', 'delete', or 'mixed'.")
