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


# Stage 1: collects all fresh data from kafka, transform metrics to parquet format and stores resulting files in 
# minio, as RAW
def stage_1():
    LOGGER.info("stage_1 >>> 2 - INFO Starting kafka consumer")
    consumer = KafkaConsumer('ddt', bootstrap_servers=['kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092'],
                         group_id="airflow",
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000)
    LOGGER.info("i'm good")

    for message in consumer:
        print('message: {} \n\n\n Value: {}'.format(message, message.value))
        LOGGER.info("message is: {}".format(message))
    return True                

TEST_VALID_APPLICATION_YAML = \
    """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: pyspark-pi
  namespace: ddt-compute
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "gcr.io/spark-operator/spark-py:v3.1.1"
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/ddt/pi.py
  sparkVersion: "3.1.1"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 3.1.1
    serviceAccount: spark-operator-spark
    volumeMounts:
      - name: config-vol
        mountPath: /opt/spark/ddt
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 3.1.1
    volumeMounts:
      - name: config-vol
        mountPath: /opt/spark/ddt
  volumes:
    - name: config-vol
      configMap:
        name: sparkapps
        items:
        - key: "pi.py"
          path: "pi.py"
      
"""

with DAG(dag_id="ddt-spark-k8s-operator", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:

    # t1 = PythonOperator(
    #     task_id='stage_1',
    #     python_callable=stage_1,
    #     dag=dag
    # )

    t1 = SparkKubernetesOperator(
        task_id='spark_pi_submit',
        namespace="ddt-compute",
        application_file=TEST_VALID_APPLICATION_YAML,
        kubernetes_conn_id="kubernetes_default",
        #do_xcom_push=True,
        dag=dag,
    )



#посмотреть логи спарк оператора