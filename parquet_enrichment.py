from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType
import time

spark = SparkSession.builder.config("spark.jars.ivy", "/tmp").appName("stage_1").getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio.ddt-persistence.svc.cluster.local")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.path.style.access", "true")
output_path = "s3a://spark/output.parquet"
checkpoint_path="s3a://spark/checkpoint"

#   should we use streaming or raw
df = spark.read.load("s3a://spark/output.parquet")
df.select(col("value").cast("string").show(100,False)).show(100,False)
#df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
spark.stop()