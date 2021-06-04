#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.2.0 --master spark://spark-master-svc:7077 --conf spark.jars.ivy=/opt/bitnami/spark/jars /tmp/test.py
# https://keestalkstech.com/2019/11/streaming-a-kafka-topic-to-a-delta-table-on-s3-with-spark-structured-streaming/
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


schema = StructType([
        StructField("@metadata", StringType()),
        StructField("@timestamp", StringType()),
        StructField("name", StringType()),
        StructField("payload", StringType()),
        StructField("well_id", StringType())
    ])

spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092")\
    .option("subscribe", "ddt").option("startingOffsets", "earliest")\
    .option("failOnDataLoss", "false")\
    .load()\
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")\
    .select(from_json(col("value").cast("string"), schema).alias("value"))\
    .select(col("value.*"))\
    .writeStream.format("parquet")\
    .outputMode("append")\
    .option("path", output_path)\
    .option("checkpointLocation", checkpoint_path)\
    .trigger(once=True)\
    .start()#.awaitTermination(60)

while spark.streams.active != []:
    print("Waiting for streaming query to finish.")
    time.sleep(20)

#spark.streams.awaitAnyTermination()
spark.stop()