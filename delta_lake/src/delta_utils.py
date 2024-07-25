import pyarrow as pa
import os
from deltalake import DeltaTable
from pyspark.sql.types import *


def convert_to_arrow_schema(schema_yaml: dict) -> pa.Schema:
    # Parse the schema
    fields = []
    for field in schema_yaml['schema']:
        field_name = field['name']
        field_type = field['type']
        
        if field_type == 'int32':
            pa_type = pa.int64()
        elif field_type == 'string':
            pa_type = pa.string()
        elif field_type == 'float64':
            pa_type = pa.float64()
        elif field_type == 'timestamp':
            pa_type = pa.timestamp('us')
        else:
            raise ValueError(f"Unsupported type: {field_type}")
        
        fields.append((field_name, pa_type))
    schema = pa.schema(fields)
    return schema

def yaml_to_spark_schema(yaml_schema):
    fields = []
    for field in yaml_schema:
        name = field['name']
        dtype = field['type']
        nullable = True  # Assuming all fields are nullable. Adjust if necessary.
        
        if dtype == 'string':
            spark_type = StringType()
        elif dtype == 'int32':
            spark_type = IntegerType()
        elif dtype == 'float64':
            spark_type = DoubleType()
        elif dtype == 'timestamp':
            spark_type = StringType()
        else:
            raise ValueError(f"Unsupported data type: {dtype}")
        
        fields.append(StructField(name, spark_type, nullable))
    
    return StructType(fields)

def is_delta_table(path):
    if os.path.exists(path):
        try:
            delta_table = DeltaTable(path)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False

        