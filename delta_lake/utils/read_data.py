import os
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
from datetime import datetime
import yaml

def dataframe_to_file(buffer, output_format='parquet', base_dir='data/scmp-raw-layer/cpo/cpis/ba_dmnd_data')->str:
    """Convert buffer to specified output format and save with current date in directory structure."""
    # Get current date in YYYYMMDD format
    current_date = datetime.now().strftime('%Y%m%d')
    year = current_date[:4]
    month = current_date[4:6].lstrip('0')  # Remove leading zeros
    day = int(current_date[6:8].lstrip('0'))+4   # Remove leading zeros
    
    # Define output directory based on current date
    output_dir = os.path.join(base_dir, f'year={year}', f'month={month}', f'day={day}')

    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output file path with current date
    output_file = os.path.join(output_dir, f'ba_dmnd_data_{current_date}_v2.{output_format}')
    
    if output_format == 'parquet':
       # Convert buffer to Arrow Table and write to Parquet file
        buffer_writer = pa.BufferReader(buffer.getvalue())
        table = pq.read_table(buffer_writer)
        pq.write_table(table, output_file)
    
    
    elif output_format == 'csv.gz':
        # Convert buffer to DataFrame and save as CSV.gz
        buffer_writer = pa.BufferReader(buffer.getvalue())
        df = pd.read_parquet(buffer_writer)
        with gzip.GzipFile(output_file, mode='w') as f:
            df.to_csv(f, index=False)
    
    elif output_format == 'html':
        # Convert buffer to DataFrame and save as HTML
        buffer_writer = pa.BufferReader(buffer.getvalue())
        df = pd.read_parquet(buffer_writer)
        with open(output_file, 'w') as f:
            f.write(df.to_html(index=False))
    
    elif output_format == 'xlsx':
        # Convert buffer to DataFrame and save as XLSX
        buffer_writer = pa.BufferReader(buffer.getvalue())
        df = pd.read_parquet(buffer_writer)
        df.to_excel(output_file, index=False)
    
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    
    print(f"Data saved to: {output_file}")
    return output_file


def read_file(file_path:str) -> pd.DataFrame:
    # Determine file format based on file extension
    file_format = file_path.split('.')[-1]
    
    # Read file into DataFrame based on format
    if file_format == 'parquet':
        df_read = pd.read_parquet(file_path)
    elif file_format == 'csv' or file_format == 'gz':
        if file_format == 'gz':
            with gzip.open(file_path, 'rt') as f:
                df_read = pd.read_csv(f)
        else:
            df_read = pd.read_csv(file_path)
    elif file_format == 'html':
        with open(file_path, 'r') as f:
            html_content = f.read()
            df_read = pd.read_html(html_content)[0]  # Assuming HTML contains a single DataFrame
    elif file_format == 'xlsx':
        df_read = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return df_read

def read_yaml(file_path):
    with open(file_path, 'r') as stream:
        try:
            yaml_data = yaml.safe_load(stream)
            return yaml_data
        except yaml.YAMLError as exc:
            print(f"Error reading YAML file: {exc}")
            return None