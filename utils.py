"""
Fitbit Sleep ETL util functions for ETL process

Written by Nicholas Cannon
"""
import requests
import logging
from airflow.models import Variable


def verify_access_token():
    """
    Check if Fitbit api token is valid. If not refresh it using the refresh
    token and update its value in airflow.
    """
    access_token = Variable.get('FITBIT_ACCESS')
    refresh_token = Variable.get('FITBIT_REFRESH')
    app_token = Variable.get('FITBIT_APP_TOKEN')

    try:
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
            logging.info('Access token valid!')  # 200 will reach here
    except Exception:
        # TODO: send email?
        logging.exception('Error checking or refreshing Fitbit tokens')


def fetch_sleep(**kwargs):
    """
    Fetch sleep data from Fitbit API for given date.
    """
    ds = kwargs.get('ds')
    access_token = Variable.get('FITBIT_ACCESS')

    try:
        logging.info(f'Fetching sleep data for {ds}')
        r = requests.get(
            f'https://api.fitbit.com/1.2/user/-/sleep/date/{ds}.json',
            headers={'Authorization': f'Bearer {access_token}'})
        r.raise_for_status()

        logging.info(r.json())
    except ValueError:
        logging.exception(f'Error parsing JSON body for date {ds}')
    except requests.HTTPError:
        logging.exception(f'Error fetching sleep data for date {ds}')
