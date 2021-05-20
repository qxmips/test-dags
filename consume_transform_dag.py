"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from kafka import KafkaConsumer
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging



def consume_kafka():
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.setLevel(logging.INFO)

    LOGGER.info("consume_kafka >>> 2 - INFO Starting kafka consumer")
    consumer = KafkaConsumer('ddt', bootstrap_servers=['kafka-cluster-kafka-bootstrap.ddt-persistence.svc.cluster.local:9092'],
                         group_id="airflow",
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000)
    LOGGER.info("I'm inside")

    for message in consumer:
      LOGGER.info("message is: {}".format(message))
    return True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('consume_transform_dag', default_args=default_args)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = PythonOperator(
    task_id='python_task',
    python_callable=consume_kafka,
    dag=dag
)



t2 >> t1
#t1.set_downstream(t2)
