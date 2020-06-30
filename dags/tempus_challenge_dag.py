import os
from datetime import timedelta

import challenge as c
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# get S3_BUCKET variable
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
          schedule_interval="@daily",  # schedule set to run daily
          catchup=False)

t1_retrieve_sources = PythonOperator(task_id='retrieve_sources_from_lang',
                                     python_callable=c.Retrieval.get_sources_lang,
                                     op_args=['en'],
                                     retries=3,
                                     dag=dag)

t2_retrieve_headlines = PythonOperator(task_id='retrieve_headlines',
                                       python_callable=c.Retrieval.get_headlines,
                                       provide_context=True,
                                       retries=3,
                                       dag=dag)

t3_upload_to_s3 = PythonOperator(task_id='upload_to_S3',
                                 python_callable=c.Retrieval.upload_to_s3,
                                 provide_context=True,
                                 op_kwargs={'S3_BUCKET': S3_BUCKET},
                                 retries=3,
                                 dag=dag)

t4_end_task = DummyOperator(task_id='end_task', dag=dag)

t1_retrieve_sources >> t2_retrieve_headlines
t2_retrieve_headlines >> t3_upload_to_s3
t3_upload_to_s3 >> t4_end_task
