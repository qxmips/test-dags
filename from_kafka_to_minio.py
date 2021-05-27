from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("stage_1") \
    .getOrCreate()

print("Creating spark df")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092") \
  .option("subscribe", "ddt") \
  .option("startingOffsets", "earliest") \
  .load()

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

if  df.isStreaming:
    print("We are streaming!")
    out = console_output(df, 5)
    out.stop()