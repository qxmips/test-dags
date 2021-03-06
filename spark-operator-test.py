# https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
import airflow
import os
from datetime import datetime, timedelta
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from kafka import KafkaConsumer
import logging
import pandas as pd

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

with DAG(dag_id="ddt-spark-k8s-operator", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
    t1 = SparkKubernetesOperator(
        task_id='stage_1_submit',
        namespace="ddt-compute",
        application_file="SparkApplication_stage_1.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )
    t2 = SparkKubernetesSensor(
        task_id='stage_1_monitor',
        namespace="ddt-compute",
        application_name="{{ task_instance.xcom_pull(task_ids='stage_1_submit')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",

    )
    t1 >> t2



#посмотреть логи спарк оператора