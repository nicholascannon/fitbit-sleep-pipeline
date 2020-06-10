"""
Fitbit Sleep ETL util functions for ETL process

Written by Nicholas Cannon
"""
import requests
import logging
from airflow.models import Variable
import os

# set up staging dir
STAGE_DIR = os.path.join(os.getcwd(), 'staging')
if not os.path.exists(STAGE_DIR):
    os.mkdir(STAGE_DIR)


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
    """
    ds = kwargs.get('ds')
    access_token = Variable.get('FITBIT_ACCESS')
    staging_dir = os.path.join(STAGE_DIR, ds)

    try:
        logging.info(f'Fetching sleep data for {ds}')
        r = requests.get(
            f'https://api.fitbit.com/1.2/user/-/sleep/date/{ds}.json',
            headers={'Authorization': f'Bearer {access_token}'})
        r.raise_for_status()
        r.json()  # validates that we actually got json data

        if not os.path.exists(staging_dir):
            os.mkdir(staging_dir)

        stage_file = os.path.join(staging_dir, f'sleep-{ds}.json')
        with open(stage_file, 'w') as f:
            f.write(r.text)

        logging.info(f'Successfully staged data to {stage_file}')
    except ValueError:
        logging.exception(f'Error parsing JSON body for date {ds}')
    except requests.HTTPError:
        logging.exception(f'Error fetching sleep data for date {ds}')
