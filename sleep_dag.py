"""
Fitbit Sleep ETL DAG

Written by Nicholas Cannon
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

import fb_sleep_etl.utils as utils

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fitbit_sleep',
    default_args=default_args,
    description='Fitbit sleep data pipeline',
    schedule_interval='@daily',
)

check_token = PythonOperator(
    task_id='verify_access_token',
    python_callable=utils.verify_access_token,
    dag=dag,
)

get_sleep = PythonOperator(
    task_id='get_sleep_data',
    python_callable=utils.fetch_sleep,
    provide_context=True,
    dag=dag,
)

check_token >> get_sleep
