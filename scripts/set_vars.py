#!/usr/bin/env python3
"""
Quick script to load in airflow variables from .env file.
"""
from airflow.models import Variable
from dotenv import load_dotenv
import os


def set_vars_from_env():
    load_dotenv()

    Variable.set('FITBIT_ACCESS', os.environ.get('FITBIT_ACCESS'))
    Variable.set('FITBIT_APP_TOKEN', os.environ.get('FITBIT_APP_TOKEN'))
    Variable.set('FITBIT_REFRESH', os.environ.get('FITBIT_REFRESH'))
    Variable.set('LOCAL_STAGING', os.environ.get('LOCAL_STAGING'))
    Variable.set('WEATHERBIT_KEY', os.environ.get('WEATHERBIT_KEY'))

    print('Airflow variables set')


if __name__ == '__main__':
    set_vars_from_env()
