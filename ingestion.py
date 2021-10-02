import airflow
import os
from datetime import datetime, timedelta
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
from kafka import KafkaConsumer
import logging
import pandas as pd

ENV = os.environ.get("ENV")
# init logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

default_args = {
            "owner": "Airflow",
            "start_date": airflow.utils.dates.days_ago(1),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "aleksandr_shitikov@example.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=1)
        }

spark_conf = { "spark.jars.ivy": "/tmp",
         "spark.executor.instances":"3",
         "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
         "spark.kubernetes.container.image": "qxmips/spark-py:3.1.1",
         "spark.kubernetes.namespace": "ddt-compute",
         "spark.kubernetes.file.upload.path": "s3a://spark/shared",
         "spark.hadoop.fs.s3a.access.key": "minio",
         "spark.hadoop.fs.s3a.secret.key": "minio123",
         "spark.hadoop.fs.s3a.endpoint": "http://minio.ddt-persistence.svc.cluster.local",
         "spark.hadoop.fs.s3a.path.style.access": "true",
         "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
         "spark.kubernetes.container.image.pullPolicy": "Always",
         "spark.ui.prometheus.enabled": "true",
         "spark.executor.processTreeMetrics.enabled":"true",
         "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
         "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
         "master.sink.prometheusServlet.path":"/metrics/master/prometheus",
         "applications.sink.prometheusServlet.path":"/metrics/applications/prometheus",
         "spark.kubernetes.executor.label.metrics-exposed":"true",
         "spark.kubernetes.driver.label.metrics-exposed":"true"
          }
             
with DAG(dag_id="ddt-ingestion", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
     
    stage_1 = SparkSubmitOperator(
        task_id="stage1",
        application="/opt/airflow/dags/repo/from_kafka_to_minio_streaming.py",
        conn_id="k8s_cluster",
        name = "stage1",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.1",
        conf = spark_conf,
        verbose=False
    )

    stage_2 = SparkSubmitOperator(
            task_id="stage2",
            application="/opt/airflow/dags/repo/parquet_enrichment.py",
            conn_id="k8s_cluster",
            name = "stage2",
            packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.1,org.postgresql:postgresql:42.2.16",
            conf = spark_conf, 
            verbose=False
        )
    stage_1 >> stage_2

#/opt/spark/bin/spark-submit --master k8s://https://8EFA8AC26455C8F75FAC76753DC7BE36.sk1.us-west-1.eks.amazonaws.com --conf spark.jars.ivy=/tmp --conf spark.executor.instances=3 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark --conf spark.kubernetes.container.image=qxmips/spark-py:3.1.1 --conf spark.kubernetes.namespace=ddt-compute --conf spark.kubernetes.container.image.pullPolicy=Always  --conf spark.kubernetes.namespace=ddt-compute --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.1.1 --name stage_1 --queue root.default --deploy-mode cluster --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark local:///opt/spark/examples/src/main/python/pi.py