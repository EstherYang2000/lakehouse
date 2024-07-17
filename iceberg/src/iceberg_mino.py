from pyiceberg import IcebergTable
from pyarrow import csv

# Define MinIO endpoint and credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "esther"
minio_secret_key = "estheresther"
minio_bucket = "iceberg_data"

# Path to Iceberg table
iceberg_table_path = f"s3a://{minio_bucket}/iceberg_table"

# Initialize IcebergTable with MinIO storage options
iceberg_table = IcebergTable(
    iceberg_table_path,
    minio_endpoint=minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
)

# Create or load Iceberg table
if not iceberg_table.exists():
    # Define schema (example schema)
    schema = {
        "fields": [
            {"name": "id", "type": "int", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
        ]
    }

    # Create table
    iceberg_table.create(schema)
else:
    # Load existing table
    iceberg_table.load()

# Display Iceberg table schema
print("Iceberg Table Schema:")
print(iceberg_table.schema())


# Example data for insertion
data_to_insert = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

# Insert data into Iceberg table
iceberg_table.insert(data_to_insert)

# Example data for update
data_to_update = [{"id": 1, "name": "Updated Alice"}]

# Update data in Iceberg table based on ID
iceberg_table.update(data_to_update, "id")

# Example data for deletion
ids_to_delete = [2]

# Delete data from Iceberg table based on ID
iceberg_table.delete(ids_to_delete, "id")

# Refresh metadata after operations
iceberg_table.refresh()
