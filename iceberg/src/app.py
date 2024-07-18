import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
import sqlite3
import pyarrow.compute as pc

warehouse_path = "data"
conn = sqlite3.connect(f'{warehouse_path}/pyiceberg_catalog.db')

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

#create table
df = pq.read_table("data/houseprice.parquet")
catalog.create_namespace("default")
table = catalog.create_table(
    "default.taxi_dataset",
    schema=df.schema,
)
table.append(df)
print(len(table.scan().to_arrow()))

df = df.append_column("per_price_area", pc.divide(df["price"], df["area"]))

print(df.schema)

with table.update_schema() as update_schema:
    update_schema.union_by_name(df.schema)
table.overwrite(df)
print(table.scan().to_arrow())
df = table.scan(row_filter="per_price_area > 10000").to_arrow()
len(df)