from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType

# Define MinIO catalog
catalog = load_catalog(
    "minio_catalog",
    type="s3",
    warehouse="s3a://scpm",
    properties={
        "fs.s3a.endpoint": "http://localhost:9000",
        "fs.s3a.access.key": "esther",
        "fs.s3a.secret.key": "estheresther",
    },
)

# Create a table schema
schema = Schema(
    NestedField.required(1, "id", LongType.get()),
    NestedField.required(2, "name", StringType.get())
)

# Create a new Iceberg table
catalog.create_table("minio_catalog.db.my_table", schema)

# Insert data into the table
from pyiceberg.table import Table

table: Table = catalog.load_table("minio_catalog.db.my_table")

with table.new_append() as append:
    append.append_file({
        "id": 1,
        "name": "Alice"
    })
    append.append_file({
        "id": 2,
        "name": "Bob"
    })
    append.commit()

# Query the table
rows = table.scan().select(["id", "name"]).collect()
for row in rows:
    print(row)
