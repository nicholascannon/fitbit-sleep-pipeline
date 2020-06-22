"""
Fitbit Sleep ETL DAG.

Written by Nicholas Cannon
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import fb_sleep_etl.utils as utils  # noqa

DAG_VERSION = 2.0

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 6, 6),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    f'fitbit_sleep_{DAG_VERSION}',
    default_args=default_args,
    description='Fitbit sleep data pipeline',
    schedule_interval='@daily',
)

# Tasks
check_token = PythonOperator(
    task_id='verify_tokens',
    python_callable=utils.verify_access_token,
    dag=dag,
)

get_sleep = PythonOperator(
    task_id='get_sleep',
    python_callable=utils.fetch_sleep,
    provide_context=True,
    dag=dag,
)

get_weather = PythonOperator(
    task_id='get_weather',
    python_callable=utils.fetch_weather,
    op_args=[Variable.get('WEATHERBIT_KEY')],
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=utils.transform,
    provide_context=True,
    dag=dag,
)

# Deps
check_token >> get_sleep
[get_sleep, get_weather] >> transform_task
