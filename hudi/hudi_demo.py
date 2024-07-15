# pyspark
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession

tableName = "trips_table"
basePath = "hdfs://namenode:8020/data/trips_table"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Hudi Demo") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://namenode:9083") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark-bundle_2.12:0.8.0") \
    .config("spark.hudi.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec") \
    .getOrCreate()

# pyspark
columns = ["ts","uuid","rider","driver","fare","city"]
data =[(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
       (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
       (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
       (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
       (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")]
inserts = spark.createDataFrame(data).toDF(*columns)

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'city'
}

inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)