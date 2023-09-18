from airflow import DAG
from datetime import datetime, timedelta

from kafka_create_topic import create_new_topic

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('airflow_kafka_nosql', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_new_topic = BranchPythonOperator(task_id='create_new_topic', python_callable=create_new_topic,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=10))

    topic_created = DummyOperator(task_id="topic_created")

    topic_exists = DummyOperator(task_id="topic_already_exists")

    create_new_topic >> [topic_created, topic_exists]
