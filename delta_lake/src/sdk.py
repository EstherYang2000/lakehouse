import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import json
import os
from delta_lake.utils.read_data import read_file,read_yaml
from delta_lake.src.delta_utils import convert_to_arrow_schema,is_delta_table
from datetime import datetime, timedelta
import argparse
import pyarrow.dataset as ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--partition', type=str, default=False, help='Partition Data')
    args = parser.parse_args()

    # Construct Delta Lake paths
    file_path = 'config/delta.yaml'  # Replace with your YAML file path
    yaml_data = read_yaml(file_path) 
    source_path = yaml_data["delta_table"]["delta_table_source_path"]
    delta_table_path = os.path.join(yaml_data["delta_table"]["delta_table_path"])
    source_df = read_file(source_path)
    # Convert all datetime64[ns] columns to datetime64[s]
    for col in source_df.select_dtypes(include=['datetime64[ns]']).columns:
        source_df[col] = source_df[col].astype('datetime64[ns]')
    schema_yaml = read_yaml('config/schema.yaml')
    schema_pa = convert_to_arrow_schema(schema_yaml)
    delta_config ={
    }

    if not os.path.exists(delta_table_path):
        print(f"Delta Table Directory '{delta_table_path}' does not exist.")
        os.makedirs(delta_table_path, exist_ok=True)
    # Check if the Delta table exists
    if is_delta_table(delta_table_path):
        print("Delta table exists at the specified path.")
        # change data 
        print("\nDelta Table Before Append Operations:")
        delta_table = DeltaTable(delta_table_path)
        print(f"Version: {delta_table.version()}")
        print(f"Files: {delta_table.files()}")
        print(len(delta_table.to_pandas()))
        print(delta_table.to_pandas())

        # Merge 
        # dt = DeltaTable(delta_table_path)
        # dt.merge(
        #     source=source_df,
        #     predicate="target.order_id = source.order_id",
        #     source_alias="source",
        #     target_alias="target") \
        # .when_not_matched_by_source_delete() \
        # .when_not_matched_insert_all() \
        # .when_matched_update_all() \
        # .execute()


        # append 
        # append_data_df = generate_fake_supply_chain_data(1)
        # for col in source_df.select_dtypes(include=['datetime64[ns]']).columns:
        #     source_df[col] = source_df[col].astype('datetime64[ns]')
        # write_deltalake(delta_table_path, source_df, mode="append")

        # overwrite
        # write_deltalake(delta_table_path, source_df, mode="overwrite")

        print("\nDelta Table After Append Operations:")
        delta_table = DeltaTable(delta_table_path)
        print(f"Version: {delta_table.version()}")
        print(f"Files: {delta_table.files()}")
        print(len(delta_table.to_pandas()))
        print(delta_table.to_pandas())

        # Read the Data 
        # 1. Converting to a PyArrow Dataset 
        # dt = DeltaTable(delta_table_path)
        # dataset = dt.to_pyarrow_dataset()
        # condition = (ds.field("supplier") == "Supplier_5") & (ds.field("quantity") > 50)
        # print(dataset.to_table(filter=condition, columns=["order_id"]))

        # 2. Read the table with version 
        # dt = DeltaTable(delta_table_path, version=2)
        # print(dt.to_pandas()) 


        # 3. Read the table history 
        dt = DeltaTable(delta_table_path)
        history = dt.history()
        df = pd.DataFrame(history)
        # Display DataFrame
        print(df.head(10))
        


    # Perform merge operation to update existing rows and insert new rows (pyspark)
        # delta_table.merge(
        #     source_df=mixed_df,
        #     condition="current_data.order_id = new_data.order_id",
        #     whenMatchedUpdate={
        #         "quantity": mixed_df["quantity"],
        #         "unit_price": mixed_df["unit_price"],
        #         "order_date": mixed_df["order_date"],
        #         "delivery_date": mixed_df["delivery_date"]
        #     },
        #     whenNotMatchedInsert={
        #         "order_id": mixed_df["order_id"],
        #         "product_id": mixed_df["product_id"],
        #         "product_name": mixed_df["product_name"],
        #         "supplier": mixed_df["supplier"],
        #         "quantity": mixed_df["quantity"],
        #         "unit_price": mixed_df["unit_price"],
        #         "order_date": mixed_df["order_date"],
        #         "delivery_date": mixed_df["delivery_date"]
        #     }
        # ).execute()

        # # Example of deleting rows not in the mixed data
        # delta_table.merge(
        #     source_df=mixed_df,
        #     condition="current_data.order_id = new_data.order_id",
        #     whenNotMatchedDelete=True
        # ).execute()
        # # Read updated state of Delta table into a DataFrame
        # delta_table_df_updated = delta_table.history(1)
        # # Display the updated state of the Delta table
        # print("\nDelta Table After Merge Operations:")
        # print(delta_table_df_updated)

    else:
        print("Delta table does not exist at the specified path.")
        # Write DataFrame to Delta Lake table in MinIO
        """ 
        How to handle existing data. 
        Default is to error if table already exists. 
        If 'append', will add new data. 
        If 'overwrite', will replace table with new data. 
        If 'ignore', will not write anything if table already exists.	

        schema_mode: overwrite 
        engine : default : pyarrow writer engine to write the delta table. Rust engine is still experimental but you may see up to 4x performance improvements over pyarrow.	
        """
        if args.partition:
            write_deltalake(delta_table_path, source_df,schema=schema_pa,storage_options=delta_config, partition_by=schema_yaml["partition"],name="ba_dmnd_data_p",schema_mode="overwrite",engine="pyarrow")
        else:
            write_deltalake(delta_table_path, source_df,schema=schema_pa,storage_options=delta_config,name="ba_dmnd_data",schema_mode="overwrite",engine="pyarrow")

