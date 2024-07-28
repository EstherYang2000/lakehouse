from pyspark.sql import SparkSession
import struct

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("Read Deletion Vector") \
    .getOrCreate()

# 讀取 Deletion Vector 二進制文件
file_path = "data/scmp-stage-layer/cpo/ba_dmnd_data_pyspark_deletion_vector/deletion_vector_4eba2019-7175-4f2d-875a-73bc93d6c29b.bin"
binary_df = spark.read.format("binaryFile").load(file_path)

# 定義解析二進制文件的 UDF
def parse_deletion_vector(binary_data):
    # 假設 Deletion Vector 格式包含：
    # 1. 4 個字節的整數表示記錄數量
    # 2. 每條記錄用 4 個字節的整數表示
    record_count = struct.unpack('I', binary_data[:4])[0]
    records = []
    for i in range(record_count):
        record_offset = 4 + i * 4
        record_id = struct.unpack('I', binary_data[record_offset:record_offset + 4])[0]
        records.append(record_id)
    return records

# 註冊 UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, BinaryType
import pyarrow.parquet as pq

parse_deletion_vector_udf = udf(parse_deletion_vector, ArrayType(IntegerType()))

# 解析二進制文件
parsed_df = binary_df.withColumn("deletion_records", parse_deletion_vector_udf(binary_df.content))

# 顯示解析後的記錄
parsed_df.select("deletion_records").show(truncate=False)
print(parsed_df)

# 停止 SparkSession
spark.stop()
