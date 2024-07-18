from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

catalog = load_catalog(
    "docs",
    **{
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "esther",
        "s3.secret-access-key": "estheresther",
    }
)
namespace = "iceberg"
catalog.create_namespace(namespace)
ns = catalog.list_namespaces()
print(catalog.list_tables(namespace))


schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4, name="created_by", field_type=StringType(), required=False
            ),
        ),
        required=False,
    ),
)
# partition_spec = PartitionSpec(
#     PartitionField(
#         source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
#     )
# )
# # Sort on the symbol
# sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
catalog.create_table(
    identifier=f"{namespace}.bids",
    schema=schema
    # location="s3://iceberg",
    # partition_spec=partition_spec,
    # sort_order=sort_order,
)
table = catalog.load_table(f"{namespace}.bids")
print(table)
# import boto3
# from pyiceberg.catalog.sql import SqlCatalog
# from pyiceberg.schema import Schema
# from pyiceberg.types import NestedField, StringType, LongType
# from pyiceberg.table import Table

# # Define MinIO endpoint and credentials
# minio_endpoint = "http://localhost:9000"
# minio_access_key = "esther"
# minio_secret_key = "estheresther"
# minio_bucket = "iceberg"
# table_name = "yfinstocks"
# schema_name = "docs_example"
# s3_location = f"s3://{minio_bucket}/{schema_name}/{table_name}"
# local_data_dir = "/tmp/stocks/"
# warehouse_path = "/tmp/warehouse"

# # Initialize Boto3 client for MinIO
# s3_client = boto3.client(
#     's3',
#     endpoint_url=minio_endpoint,
#     aws_access_key_id=minio_access_key,
#     aws_secret_access_key=minio_secret_key
# )

# # Initialize the catalog
# catalog = SqlCatalog(
#     name="docs",
#     # uri=f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
#     warehouse=f"s3://{minio_bucket}",
#     s3={
#         "s3.endpoint": minio_endpoint,
#         "s3.access-key-id": minio_access_key,
#         "s3.secret-access-key": minio_secret_key,
#     }
# )

# try:
#     table = catalog.create_table(
#         f'{schemaname}.{tablename}',
#         schema=df.schema,
#         location=s3location,
#     )
# except:
#     print("Table exists, append " + tablename)    
#     table = catalog.load_table(f'{schemaname}.{tablename}')



# # Initialize Boto3 client for MinIO (if needed directly for other operations)
# s3_client = boto3.client(
#     's3',
#     endpoint_url=minio_endpoint,
#     aws_access_key_id=minio_access_key,
#     aws_secret_access_key=minio_secret_key
# )

# # Initialize HadoopCatalog for S3-compatible storage
# catalog = HadoopCatalog(
#     warehouse=f"s3a://{minio_bucket}/",
#     hadoop_conf={
#         "fs.s3a.access.key": minio_access_key,
#         "fs.s3a.secret.key": minio_secret_key,
#         "fs.s3a.endpoint": minio_endpoint,
#         "fs.s3a.path.style.access": True
#     }
# )

# # Define schema
# schema = Schema(
#     NestedField(id=1, name="id", field_type=LongType(), required=True),
#     NestedField(id=2, name="data", field_type=StringType(), required=True)
# )

# # Create or load a table
# table_name = f"{minio_bucket}.my_table"
# if catalog.table_exists(table_name):
#     table = catalog.load_table(table_name)
# else:
#     table = catalog.create_table(
#         identifier=table_name,
#         schema=schema,
#         location=f"s3a://{minio_bucket}/my_table/"
#     )

# # Example DML operations
# # Insert data
# rows = [
#     {"id": 1, "data": "foo"},
#     {"id": 2, "data": "bar"}
# ]
# for row in rows:
#     append = table.new_append()
#     append.append(row)
#     append.commit()

# # Query data
# for row in table.scan():
#     print(row)

# # Update data
# overwrite = table.new_overwrite()
# overwrite.overwrite({"id": 1}, {"data": "baz"})
# overwrite.commit()

# # Delete data
# delete = table.new_delete()
# delete.delete({"id": 2})
# delete.commit()
