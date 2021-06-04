from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType
import time

spark = SparkSession.builder.config("spark.jars.ivy", "/tmp").config("spark.sql.sources.partitionOverwriteMode","dynamic").appName("stage_1").getOrCreate()
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
df.show(10,False)
df.write.partitionBy("well_id").format("parquet").save("s3a://spark/output2.parquet")
spark.stop()