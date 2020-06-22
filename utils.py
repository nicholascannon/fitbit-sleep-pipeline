"""
Fitbit Sleep ETL util functions for ETL process

Written by Nicholas Cannon
"""
import os
import requests
import logging
import json
from datetime import datetime as dt
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def verify_access_token():
    """
    Check if Fitbit api token is valid. If not refresh it using the refresh
    token and update its value in airflow.
    """
    try:
        access_token = Variable.get('FITBIT_ACCESS')
        refresh_token = Variable.get('FITBIT_REFRESH')
        app_token = Variable.get('FITBIT_APP_TOKEN')

        logging.info('Verifying access token')
        r = requests.get('https://api.fitbit.com/1/user/-/profile.json',
                         headers={'Authorization': f'Bearer {access_token}'})

        if r.status_code == 401:  # Refresh access token
            logging.info('Access token requires refresh, refreshing now')
            r = requests.post(
                'https://api.fitbit.com/oauth2/token',
                headers={
                    'Authorization': f'Basic {app_token}',
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                })
            r.raise_for_status()

            body = r.json()
            access_token = body.get('access_token')
            refresh_token = body.get('refresh_token')

            if not access_token or not refresh_token:
                raise Exception(f'Could not get new tokens from Fitbit: {body}')

            logging.info('Token refresh successful! Saving to airflow')
            Variable.set('FITBIT_ACCESS', access_token)
            Variable.set('FITBIT_REFRESH', refresh_token)
        else:
            r.raise_for_status()
            logging.info('Access token valid!')
    except KeyError:
        logging.exception('Fitbit credentials do not exist')
    except requests.HTTPError:
        logging.exception('Fitbit token validation error')
    except ValueError:
        logging.exception('Could not parse json body')


def fetch_sleep(**kwargs):
    """
    Fetch sleep data from Fitbit API for the given execution date and store in
    the staging area.

    Sleep API docs: https://dev.fitbit.com/build/reference/web-api/sleep/
    """
    ds = kwargs.get('ds')
    access_token = Variable.get('FITBIT_ACCESS')

    try:
        logging.info(f'Fetching sleep data for {ds}')
        r = requests.get(
            f'https://api.fitbit.com/1.2/user/-/sleep/date/{ds}.json',
            headers={'Authorization': f'Bearer {access_token}'})
        r.raise_for_status()
        r.json()  # validates that we actually got json data

        # TODO: send to GCS
    except ValueError:
        logging.exception(f'Error parsing JSON body for date {ds}')
    except requests.HTTPError:
        logging.exception(f'Error fetching sleep data for date {ds}')


def fetch_weather(api_key, **kwargs):
    """
    Fetch daily weather data for execution date.

    Weather API docs: https://www.weatherbit.io/api/weather-history-daily
    """
    ds = kwargs.get('ds')
    tomorrow_ds = kwargs.get('tomorrow_ds')

    try:
        r = requests.get(
            f'https://api.weatherbit.io/v2.0/history/daily?city=Perth&country=AU&start_date={ds}&end_date={tomorrow_ds}&key={api_key}')
        r.raise_for_status()
        r.json()

        # TODO: Send to GCS
    except ValueError:
        logging.exception('Error parsing JSON from weather api')
    except requests.HTTPError:
        logging.exception('Error fetching weather data')


def process_sleep(data):
    """
    Clean staged Fitbit sleep data. Extract main sleep log and combine sleep
    events and sort by datetime stamp.
    """
    clean = {}

    # extact main sleep log
    for log in data['sleep']:
        if log['isMainSleep']:
            clean = log
            break
    else:
        # todo: some kind of alerting here!!
        raise Exception('No sleep log for date!')

    # combine sleep events and sort by dateTime entry
    clean['events'] = clean['levels']['data'] + clean['levels']['shortData']
    clean['events'] = sorted(clean['events'],
                             key=lambda x: dt.strptime(x['dateTime'], '%Y-%m-%dT%H:%M:%S.%f'))
    clean['events'] = json.dumps(clean['events'])
    return clean


def transform(**kwargs):
    """
    Clean and transform Fitbit and weather data from staging area and load into
    postgres table.
    """
    ds = kwargs.get('ds')
    pg_hook = PostgresHook(postgres_conn_id='sleep_dw')

    # TODO: load from GCS

    # clean staged data
    sleep = process_sleep(sleep)
    summary = sleep['levels']['summary']
    weather = weather['data'][0]

    # load into datawarehouse
    sleep_query = """INSERT INTO daily_sleep_data
    (ds, efficiency, startTime, endTime, events, deep, light, rem, wake,
    minAfterWakeup, minAsleep, minAwake, minInBed, temp, maxTemp, minTemp,
    precip)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    data = (ds, sleep['efficiency'], sleep['startTime'], sleep['endTime'],
            sleep['events'], summary['deep']['minutes'], summary['light']['minutes'],
            summary['rem']['minutes'], summary['wake']['minutes'],
            sleep['minutesAfterWakeup'], sleep['minutesAsleep'], sleep['minutesAwake'],
            sleep['timeInBed'], weather['temp'], weather['max_temp'], weather['min_temp'],
            weather['precip'])

    pg_hook.run(sleep_query, parameters=data)
