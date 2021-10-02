from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr, lit, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import spark_partition_id, asc, desc, current_timestamp

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

#   should we use streaming or raw
df = spark.read.load("s3a://spark/output.parquet")
df_enriched = df.withColumn("partition_id", spark_partition_id()).withColumn("timestamp",current_timestamp()) 
df_enriched.show(10,False)
df_enriched.write.mode("overwrite").format("parquet").save("s3a://spark/curated/output.parquet")


#m7 partially skipped
#m7  Spark should report partition row count on each landing to raw transformation, both read and written
#countByPartitionRead = df.groupBy(spark_partition_id()).count().withColumn("@timestamp", current_timestamp().cast("String")).withColumn("stage",  lit("read"))
#countByPartitionWritten = df_enriched.groupBy(spark_partition_id()).count().withColumn("@timestamp", current_timestamp().cast("String")).withColumn("stage",  lit("write"))
#countByPartitionRead.write.format("org.elasticsearch.spark.sql").option("es.nodes", "elasticsearch-master.ddt-observability.svc.cluster.local:9200").option("es.index.auto.create", "true").mode("overwrite").save("spark-read/_doc")
#countByPartitionWritten.write.format("org.elasticsearch.spark.sql").option("es.nodes", "elasticsearch-master.ddt-observability.svc.cluster.local:9200").option("es.index.auto.create", "true").mode("overwrite").save("spark-write/_doc")

# print('*' * 50)
# for i in sorted(countByPartitionRead, key=lambda x: x[1]):
#         print(i)
# print('*' * 50)

#m10
json_schema = spark.read.json(df.rdd.map(lambda row: row.payload)).schema
df2 = df_enriched.withColumn('value', from_json(col('payload'), json_schema)['value']).drop('payload')

json_schema = spark.read.json(df.rdd.map(lambda row: row.payload)).schema
df2 = df.withColumn('value', from_json(col('payload'), json_schema)['value']).drop('payload')

def parq_hourly_avg(df,name):
    """Returns the dataframe with hourly aggregated metrics 
    and writes data to a parquet file partitioned by date
    """
    df_avg =  (df2.filter(df.name == name)
              .withColumn("year", year(col("@timestamp")))
              .withColumn("month", month(col("@timestamp")))
              .withColumn("day", dayofmonth(col("@timestamp")))
              .withColumn("hour", hour(col("@timestamp")))
              .groupBy("well_id","name","year","month","day","hour")
              .avg("value")
              .withColumnRenamed("avg(value)", 'hourly_avg_value'))
    #df_avg.show()
    df_avg.write.partitionBy("year","month","day","hour").mode("overwrite").format("parquet").save("s3a://spark/hourly/" + name + "/output.parquet")
    return df_avg

df_well_pipe_pressure = parq_hourly_avg(df2,"well_pipe_pressure")
df_well_generator_temperature = parq_hourly_avg(df2,"well_generator_temperature")
df_well_natural_gas = parq_hourly_avg(df2,"well_natural_gas")
df_well_generator_power =  parq_hourly_avg(df2,"well_generator_power")
df_well_crude_oil = parq_hourly_avg(df2,"well_crude_oil")
df_well_pump_temperature = parq_hourly_avg(df2,"well_pump_temperature")
df_well_pipe_temperature = parq_hourly_avg(df2,"well_pipe_temperature")
df_well_pump_horse_power =parq_hourly_avg(df2,"well_pump_horse_power")
df_well_pump_rpm  = parq_hourly_avg(df2,"well_pump_rpm")

mode = "append"
jdbc_url="jdbc:postgresql://rdb-postgresql-ha-pgpool.ddt-persistence.svc.cluster.local:5432/pipe_pressure"
config = {"user":"airflow", 
          "password": "zNrnHq%v",
          "driver":"org.postgresql.Driver"}
 
df_well_pipe_pressure.write.jdbc(url=jdbc_url, table='well_pipe_pressure', mode=mode, properties=config)
spark.stop()