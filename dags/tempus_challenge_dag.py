import os

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import challenge as c

S3_BUCKET = os.getenv("S3_BUCKET")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['pavanamin93@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('tempus_challenge_dag',
            default_args=default_args,
            schedule_interval="@daily", #schedule set to run daily
            catchup=False)

t1_Retrieve_Sources = PythonOperator(task_id='retrieve_sources_from_lang',
    python_callable=c.Retrieval.get_sources_lang,
    op_args=['en'],
    retries = 3,
    dag=dag)

t2_Retrieve_Headlines = PythonOperator(task_id = 'retrieve_headlines',
    python_callable=c.Retrieval.get_headlines,
    provide_context=True,
    retries = 3,
    dag=dag)

t3_Upload_to_S3 = PythonOperator(task_id='upload_to_S3',
    python_callable=c.Retrieval.upload_to_S3,
    provide_context=True,
    op_kwargs={'S3_BUCKET' : S3_BUCKET},
    retries = 3,
    dag=dag)

t4_End_Task = DummyOperator(task_id='end_task', dag=dag)

t1_Retrieve_Sources >> t2_Retrieve_Headlines
t2_Retrieve_Headlines >> t3_Upload_to_S3
t3_Upload_to_S3 >> t4_End_Task
