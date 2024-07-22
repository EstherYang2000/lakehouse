import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
import os
from delta_lake.utils.read_data import read_file,read_yaml

if __name__ == "__main__":
    # Construct Delta Lake paths
    file_path = 'config/delta.yaml'  # Replace with your YAML file path
    yaml_data = read_yaml(file_path) 
    source_path = yaml_data["delta_table"]["delta_table_source_path"]
    table_name = source_path.split('/')[-1].split('.')[-2]
    delta_table = os.path.join(yaml_data["delta_table"]["delta_table_path"],)
    delta_table_path = f"lakehouse/delta_lake/data/scmp-stage-layer/cpo/ba_dmnd_data/{table_name}"

    if not os.path.exists(delta_table_path):
        print(f"Delta Table Directory '{directory}' does not exist.")
        os.makedirs(delta_table_path, exist_ok=True)
        
    else:


        # Define schema for the Delta Lake table 
        # schema = pa.schema([
        #     ("id", pa.int32()),
        #     ("name", pa.string())
        # ])
        # Create a Pandas DataFrame
        # df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        # df = df.astype({"id": "int32", "name": "string"})  # Ensure Pandas dtype matches Spark schema

        # Write DataFrame to Delta Lake table in MinIO
        # write_deltalake(delta_table_path, df,schema=schema,storage_options=delta_config)
