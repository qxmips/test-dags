#https://github.com/ThulasitharanGT/KafkaGradleTest/blob/a89fc57dedf4b6fb75d5595205dc4c7caa2e6f4a/commandsAndDDL.txt
#https://github.com/apnmrv/otus-de-2019-08-public/blob/36c6320b1b5e03a4ea197fe14c85ce8326264320/09_25_wikiflow/consumer/Dockerfile-structured
#--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_
# wget http://maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar
# https://github.com/bitnami/bitnami-docker-spark
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.2.0 --master spark://spark-master-svc:7077 --conf spark.jars.ivy=/opt/bitnami/spark/jars /tmp/test.py
#helm install spark -n ddt-compute bitnami/spark --set image.repository=qxmips/spark --set image.tag=3.1.1 --set image.pullPolicy=Always
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType
import time

#        config("spark.hadoop.fs.s3a.access.key", os.environ.get('AWS_KEY_ID')).\
#        config("spark.hadoop.fs.s3a.secret.key", os.environ.get('AWS_SECRET')).\

spark = SparkSession.builder.config("spark.jars.ivy", "/opt/bitnami/spark/jars").appName("stage_1").getOrCreate()

#https://gist.github.com/tobilg/e03dbc474ba976b9f235
#https://github.com/spartonia/HemnetProject/blob/42a7f008f9a91838392cc877e7b553447e3d6b1a/dags/hemnet_daily_forsale_workflow.py
#https://github.com/SureshBoddu-DataEng/Spark-Streaming-Examples/blob/bc9ab16e739ce9de093d641be0016850d289792b/com/dsm/kafka/append_mode_demo.py
#--packages org.apache.hadoop:hadoop-aws:3.2.0
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio.ddt-persistence.svc.cluster.local")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.path.style.access", "true")
output_path = "s3a://spark/output.parquet"
checkpoint_path="s3a://spark/checkpoint"

#print("Creating static df")
#static_spark_reader = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092").option("subscribe", "ddt").option("startingOffsets", "earliest").load()
#static_spark_reader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("parquet").mode("Overwrite").save(output_path)

schema = StructType([
        StructField("@metadata", StringType()),
        StructField("@timestamp", StringType()),
        StructField("name", StringType()),
        StructField("payload", StringType()),
        StructField("well_id", StringType())
    ])



print("Creating static df")
static_spark_reader = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092").option("subscribe", "ddt").option("startingOffsets", "earliest").load()
#static_spark_reader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("parquet").mode("Overwrite").save(output_path)
static_spark_reader.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")\
    .select(from_json(col("value").cast("string"), schema).alias("value"))\
    .write.format("parquet")\
    .mode("append")\
    .option("checkpointLocation", checkpoint_path)\
    .option("path", output_path)\
    .save()

"""
spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092")\
    .option("subscribe", "ddt").option("startingOffsets", "latest")\
    .load()\
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")\
    .select(from_json(col("value").cast("string"), schema).alias("value"))\
    .writeStream.format("parquet")\
    .outputMode("append")\
    .option("path", output_path)\
    .option("checkpointLocation", checkpoint_path)\
    .trigger(processingTime='5 seconds')\
    .start().awaitTermination(60)
"""

"""
print("Creating spark stream df")
stream_spark_reader = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092").option("subscribe", "ddt").option("startingOffsets", "latest").load()
value_df = stream_spark_reader.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value").select(from_json(col("value").cast("string"), schema).alias("value"))
value_df.printSchema()
#output = value_df.writeStream.format("console").option("truncate","false").outputMode("append").trigger(processingTime="5 seconds").start()
output =  value_df.writeStream.format("parquet").outputMode("append").option("path", output_path).option("checkpointLocation", checkpoint_path).trigger(processingTime='1 second').start()
output.awaitTermination(60)
"""
#spark-kafka-relation-ec8e8bed-75f0-4d07-8f9b-9b01f22ab085-driver-0
#  client.id = consumer-spark-kafka-relation-ec8e8bed-75f0-4d07-8f9b-9b01f22ab085-driver-0-1
spark.stop()