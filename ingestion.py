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
             
with DAG(dag_id="ddt-ingestion", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
     
    stage_1 = SparkSubmitOperator(
        task_id="stage_1",
        application="/opt/airflow/dags/repo/from_kafka_to_minio_streaming.py",
        conn_id="k8s_cluster",
        name = "stage_1",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.7.0,org.apache.hadoop:hadoop-aws:3.2.0",
        conf = { #"spark.jars.ivy": "/tmp",
         "spark.executor.instances":"3",
         "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
         "spark.kubernetes.container.image": "qxmips/spark-py:3.1.1",
         "spark.kubernetes.namespace": "ddt-compute"
          }, 
        verbose=False
    )


#посмотреть логи спарк оператора