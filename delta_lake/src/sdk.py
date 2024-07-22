import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
import os
from delta_lake.utils.read_data import read_file,read_yaml
from delta_lake.src.delta import convert_to_arrow_schema,is_delta_table
from datetime import datetime, timedelta

if __name__ == "__main__":
    # Construct Delta Lake paths
    file_path = 'config/delta.yaml'  # Replace with your YAML file path
    yaml_data = read_yaml(file_path) 
    source_path = yaml_data["delta_table"]["delta_table_source_path"]
    table_name = source_path.split('/')[-1].split('.')[-2]
    delta_table = os.path.join(yaml_data["delta_table"]["delta_table_path"],)
    delta_table_path = f"data/scmp-stage-layer/cpo/ba_dmnd_data/{table_name}"
    source_df = read_file(source_path)
    # Convert all datetime64[ns] columns to datetime64[s]
    print(source_df.dtypes)
    for col in source_df.select_dtypes(include=['datetime64[ns]']).columns:
        source_df[col] = source_df[col].astype('datetime64[ns]')
    schema_yaml = read_yaml('config/schema.yaml')
    schema_pa = convert_to_arrow_schema(schema_yaml)
    print(schema_pa)
    delta_config ={}
    if not os.path.exists(delta_table_path):
        print(f"Delta Table Directory '{delta_table_path}' does not exist.")
        os.makedirs(delta_table_path, exist_ok=True)
    # Check if the Delta table exists
    if is_delta_table(delta_table_path):
        print("Delta table exists at the specified path.")
    else:
        print("Delta table does not exist at the specified path.")
        # Write DataFrame to Delta Lake table in MinIO
        write_deltalake(delta_table_path, source_df,schema=schema_pa,storage_options=delta_config)
        