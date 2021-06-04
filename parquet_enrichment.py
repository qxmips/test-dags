from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import spark_partition_id, asc, desc, current_timestamp
import time
import time
import datetime
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
df_enriched = df.withColumn("partition_id", spark_partition_id()).withColumn("timestamp",current_timestamp()) 
df_enriched.show(10,False)
df_enriched.write.mode("append").format("parquet").save("s3a://spark/curated/output.parquet")
spark.stop()