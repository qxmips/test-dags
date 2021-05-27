#--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_
# wget http://maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar
# https://github.com/bitnami/bitnami-docker-spark

from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1").appName("stage_1").getOrCreate()

print("Creating spark df")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092").option("subscribe", "ddt").option("startingOffsets", "earliest").load()
def console_output(df, freq):
    return df.writeStream.format("console").trigger(processingTime='%s seconds' % freq ).options(truncate=True).start()

if  df.isStreaming:
    print("We are streaming!")
    out = console_output(df, 5)
    out.stop()