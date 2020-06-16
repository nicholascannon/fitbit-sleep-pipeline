"""
Fitbit Sleep ETL DAG.

Written by Nicholas Cannon
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import fb_sleep_etl.utils as utils

DAG_VERSION = 1.0

default_args = {
    'owner': 'airflow',
    'start_date': '2020-06-06',
    'depends_on_past': False,
}
dag = DAG(
    f'fitbit_sleep_{DAG_VERSION}',
    default_args=default_args,
    description='Fitbit sleep data pipeline',
    schedule_interval='@daily',
)

# Tasks
setup_staging = PythonOperator(
    task_id='setup_staging',
    python_callable=utils.setup_staging,
    provide_context=True,
    dag=dag,
)

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
setup_staging >> [check_token, get_weather]
check_token >> get_sleep
[get_sleep, get_weather] >> transform_task
