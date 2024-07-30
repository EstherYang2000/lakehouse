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
        'order_id': [f'ORD{str(i).zfill(4)}' for i in range(1001, num_records+1001)],
        'product_id': [f'PROD{str(i).zfill(4)}' for i in range(1001, num_records+1001)],
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

    
def manipulate_data(df,fake_mode, mode):
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
        if fake_mode == 'multi_rows':
            new_data = generate_fake_supply_chain_data(1000000)
        elif fake_mode == 'multi_cols':
            new_data = generate_fake_mul_cols_supply_chain_data(100000)
        updated_df = manipulate_data(df,fake_mode,"update")
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
            deleted_df = manipulate_data(df_copy,fake_mode,"delete")
            mixed_df = manipulate_data(deleted_df,fake_mode,"upsert")
            return mixed_df
        else:

            return df_copy
        
    else:
        raise ValueError("Invalid mode. Choose from 'update', 'upsert', 'delete', or 'mixed'.")


def generate_fake_mul_cols_supply_chain_data(num_records):
    """Generate fake supply chain data DataFrame."""
    data = {
        'order_id': [f'ORD{str(i).zfill(6)}' for i in range(1001, num_records + 1001)],
        'product_id': [f'PROD{str(i).zfill(6)}' for i in range(1001, num_records + 1001)],
        'product_name': [f'Product_{i}' for i in range(1, num_records + 1)],
        'supplier': [f'Supplier_{random.randint(1, 10)}' for _ in range(num_records)],
        'quantity': [random.randint(1, 100) for _ in range(num_records)],
        'unit_price': [round(random.uniform(10.0, 100.0), 2) for _ in range(num_records)],
        'order_date': [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(num_records)],
        'delivery_date': [datetime.now() + timedelta(days=random.randint(1, 30)) for _ in range(num_records)]
    }
    
    # Adding additional 42 columns with random data
    for i in range(1, 43):
        column_name = f'column_{i}'
        if i % 2 == 0:
            data[column_name] = [random.choice(['A', 'B', 'C', 'D', 'E']) for _ in range(num_records)]
        else:
            data[column_name] = [int(random.randint(1, 1000)) for _ in range(num_records)]
    
    return pd.DataFrame(data)