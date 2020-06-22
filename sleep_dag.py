"""
Fitbit Sleep ETL DAG.

Written by Nicholas Cannon
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import fb_sleep_etl.utils as utils  # noqa

DAG_VERSION = 2.0
GCS_BUCKET = 'sleep-staging'

dag = DAG(
    f'fitbit_sleep_{DAG_VERSION}',
    description='Fitbit sleep data pipeline',
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2020, 6, 6),
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
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
upload_sleep = FileToGoogleCloudStorageOperator(
    task_id='upload_sleep_to_gcs',
    src='{}/{}-sleep.json'.format(Variable.get('LOCAL_STAGING'), '{{ds}}'),
    dst='{{ds}}/sleep.json',
    bucket=GCS_BUCKET,
    google_cloud_storage_conn_id='sleep-gcp',
    on_success_callback=utils.remove_temp('sleep'),
    dag=dag,
)

get_weather = PythonOperator(
    task_id='get_weather',
    python_callable=utils.fetch_weather,
    op_args=[Variable.get('WEATHERBIT_KEY')],
    provide_context=True,
    dag=dag,
)
upload_weather = FileToGoogleCloudStorageOperator(
    task_id='upload_weather_to_gcs',
    src='{}/{}-weather.json'.format(Variable.get('LOCAL_STAGING'), '{{ds}}'),
    dst='{{ds}}/weather.json',
    bucket=GCS_BUCKET,
    google_cloud_storage_conn_id='sleep-gcp',
    on_success_callback=utils.remove_temp('weather'),
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=utils.transform,
    provide_context=True,
    dag=dag,
)

# Deps
check_token >> get_sleep >> upload_sleep
get_weather >> upload_weather
[upload_sleep, upload_weather] >> transform_task
