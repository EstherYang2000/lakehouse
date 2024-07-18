import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
import sqlite3
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
warehouse_path = "data"
conn = sqlite3.connect(f'{warehouse_path}/pyiceberg_catalog.db')

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
) 
# Define the initial schema for your Iceberg table https://py.iceberg.apache.org/api/#write-support
initial_schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string())
]) 
##create table
# df = pq.read_table("data/namelist.parquet")
# catalog.create_namespace("iceberg")
# table = catalog.create_table(
#     "iceberg.namelist_dataset",
#     schema=initial_schema,
# )
# table.append(df)
# print(len(table.scan().to_arrow()))

#
table = catalog.load_table("iceberg.namelist_dataset")
# Update schema to include new columns
# with table.update_schema() as update:
    # update.update_column("bid", field_type=DoubleType())

#    update.delete_column("age")
    # Add columns
    # update.add_column("age", StringType(), "Number of retries to place the bid")
    # # In a struct
    # update.add_column("details.confirmed_by", StringType(), "Name of the exchange")
    # # Rename column
    # update.rename("retries", "num_retries")
    # # This will rename `confirmed_by` to `exchange`
    # update.rename("properties.confirmed_by", "exchange")

# Optionally, print the updated schema of the table
print(table.schema())

# Example: Perform upsert operation to insert new data or update existing data
# Assume df_upsert contains data to be upserted
# data = {
#     "id": [1,101, 102],
#     "name": ["Alice","John", "Jane"]
#     # "age": ["50","30", "28"]  # Assuming this is the new column to upsert
# }

# df_upsert = pd.DataFrame(data)
# df_upsert = df_upsert.astype({"id": "int32", "name": "string"})
# table_upsert = pa.Table.from_pandas(df_upsert)
# #Append the converted PyArrow Table to the Iceberg table
# table.update(table_upsert)

# Define the condition to identify rows to delete
delete_condition = "id = 1"
# Use a transaction to delete rows based on the condition

# Scan the table and filter rows to keep
rows_to_keep = table.scan().to_pandas()
rows_to_keep = rows_to_keep[~rows_to_keep["id"].isin([1])]
print(rows_to_keep)
# Convert the filtered DataFrame back to PyArrow Table
table_to_keep = pa.Table.from_pandas(rows_to_keep)

# Overwrite the Iceberg table with the filtered data
table.overwrite(table_to_keep)

# df = df.append_column("per_price_area", pc.divide(df["price"], df["area"]))

# print(df.schema)

# table.overwrite(df)
print(table.scan().to_arrow().to_pandas())
# df = table.scan(row_filter="per_price_area > 10000").to_arrow()
# len(df)