# Fitbit Sleep ETL Pipeline

Fitbit ETL pipeline built using Airflow, Postgres & GCP. Extracts my daily sleep information from the fitbit API and weather data for the same date then stages this data in Google Cloud Storage, pulls it processes & cleans it and then loads into Postgres. Pipeline automatically verifies and refreshes fitbit OAuth2.0 tokens. Weather data collected from [Weatherbit](https://www.weatherbit.io/api).

## Installation

There are some simple script files to help in the setup process.

1. Clone the repo into your airflow dag bag. Ensure that the files are cloned into a parent folder named `fb_sleep_etl/` so that the dag definition file can import the `utils.py` file correctly.

2. Set up [fitbit OAuth](https://dev.fitbit.com/build/reference/web-api/oauth2/)

3. Add required variables to airflow (**note: anyone with access to the airflow ui can see these!!**)

   - Add weather bit key as `'WEATHERBIT_KEY'`
   - Add fitbit access token as `'FITBIT_ACCESS'`
   - Add fitbit refresh token as `'FITBIT_REFRESH'`
   - Add base64 encoded app name and secret (joined with `:`) as `'FITBIT_APP_TOKEN'`
   - Add the local file staging directory as `LOCAL_STAGING`

   Use the script `set_vars.py` to set the required variables from a `.env` file.

4. Provision a Postgres data warehouse with the schema below (I've provided a script `start_dw.sh` that runs a postgres db locally via docker and creates the required tables).

5. Add an Airflow Postgres connection with the connection id of `sleep-dw` and a Google Cloud Platform connection with the id `sleep-gcp` (Requires creating a provisioning account with GCS permissions).

## DAG

![dag](https://i.imgur.com/OjxD3PR.png)

## Task Duration

![task_duration](https://i.imgur.com/0F1vtDc.png)

## The Final Table

I've used a denormalized table as this is better for analytics.

```
CREATE TABLE IF NOT EXISTS daily_sleep_data (
    ID SERIAL PRIMARY KEY NOT NULL,
    ds DATE NOT NULL,
    efficiency INT,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    events JSON,
    deep INT,
    light INT,
    rem INT,
    wake INT,
    minAfterWakeup INT,
    minAsleep INT,
    minAwake INT,
    minInBed INT,
    temp DECIMAL,
    maxTemp DECIMAL,
    minTemp DECIMAL,
    precip DECIMAL
);
```

## Raw Sleep Data

```
{
  "sleep": [
    {
      "dateOfSleep": "2020-06-06",
      "duration": 21180000,
      "efficiency": 56,
      "endTime": "2020-06-06T06:49:00.000",
      "infoCode": 0,
      "isMainSleep": true,
      "levels": {
        "data": [
          {"dateTime": "2020-06-06T00:56:00.000", "level": "wake", "seconds": 300},
          {"dateTime": "2020-06-06T01:01:00.000", "level": "light", "seconds": 2580},
          {"dateTime": "2020-06-06T01:44:00.000", "level": "rem", "seconds": 870},
          {"dateTime": "2020-06-06T01:58:30.000", "level": "light", "seconds": 420},
          {"dateTime": "2020-06-06T02:05:30.000", "level": "deep", "seconds": 3870},
          {"dateTime": "2020-06-06T03:10:00.000", "level": "rem", "seconds": 1470},
          {"dateTime": "2020-06-06T03:34:30.000", "level": "light", "seconds": 4920},
          {"dateTime": "2020-06-06T04:56:30.000", "level": "rem", "seconds": 1260},
          {"dateTime": "2020-06-06T05:17:30.000", "level": "light", "seconds": 2610},
          {"dateTime": "2020-06-06T06:01:00.000", "level": "wake", "seconds": 330},
          {"dateTime": "2020-06-06T06:06:30.000", "level": "light", "seconds": 2160},
          {"dateTime": "2020-06-06T06:42:30.000", "level": "wake", "seconds": 390}
        ],
        "shortData": [
          {"dateTime": "2020-06-06T01:17:00.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T01:38:30.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T01:59:30.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T03:09:30.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T03:43:00.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T03:49:00.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T04:24:30.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T04:39:00.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T04:43:00.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T04:47:30.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T04:53:00.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T04:55:30.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T05:16:00.000", "level": "wake", "seconds": 90},
          {"dateTime": "2020-06-06T05:25:30.000", "level": "wake", "seconds": 180},
          {"dateTime": "2020-06-06T05:41:30.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T05:51:00.000", "level": "wake", "seconds": 60},
          {"dateTime": "2020-06-06T06:11:00.000", "level": "wake", "seconds": 30},
          {"dateTime": "2020-06-06T06:33:30.000", "level": "wake", "seconds": 60}
        ],
        "summary": {
          "deep": {"count": 1, "minutes": 64, "thirtyDayAvgMinutes": 75},
          "light": {"count": 20, "minutes": 196, "thirtyDayAvgMinutes": 240},
          "rem": {"count": 3, "minutes": 59, "thirtyDayAvgMinutes": 89},
          "wake": {"count": 21, "minutes": 34, "thirtyDayAvgMinutes": 65}
        }
      },
      "logId": 27503647818,
      "minutesAfterWakeup": 20,
      "minutesAsleep": 319,
      "minutesAwake": 34,
      "minutesToFallAsleep": 0,
      "startTime": "2020-06-06T00:56:00.000",
      "timeInBed": 353,
      "type": "stages"
    }
  ],
  "summary": {
    "stages": {"deep": 64, "light": 196, "rem": 59, "wake": 34},
    "totalMinutesAsleep": 319,
    "totalSleepRecords": 1,
    "totalTimeInBed": 353
  }
}

```

## Raw Weather Data

```
{
  "timezone": "Australia/Perth",
  "state_code": "08",
  "country_code": "AU",
  "lat": -31.95224,
  "lon": 115.8614,
  "city_name": "Perth",
  "station_id": "946080-99999",
  "data": [
    {
      "rh": 50.8,
      "max_wind_spd_ts": 1591387200,
      "t_ghi": 3356,
      "max_wind_spd": 6.5,
      "solar_rad": 35,
      "wind_gust_spd": 6.5,
      "max_temp_ts": 1591423200,
      "min_temp_ts": 1591380000,
      "clouds": 100,
      "max_dni": 797.1,
      "precip_gpm": 0,
      "wind_spd": 3.3,
      "slp": 1017.7,
      "ts": 1591372800,
      "max_ghi": 551.8,
      "temp": 18.6,
      "pres": 1011.8,
      "dni": 261.7,
      "dewpt": 8.6,
      "snow": 0,
      "dhi": 31.1,
      "precip": 5.1,
      "wind_dir": 84,
      "max_dhi": 97.8,
      "ghi": 139.8,
      "max_temp": 24,
      "t_dni": 6281.9,
      "max_uv": 1.8,
      "t_dhi": 746.5,
      "datetime": "2020-06-06",
      "t_solar_rad": 839,
      "min_temp": 16.9,
      "max_wind_dir": 195,
      "snow_depth": null
    }
  ],
  "sources": [
    "946140-99999",
    "946080-99999",
    "ASN00009034",
    "ASN00009159",
    "ASN00009126",
    "ASN00009225",
    "ASN00009094",
    "ASN00009035",
    "ASN00009161",
    "ASN00009056",
    "ASN00009151",
    "ASN00009012",
    "ASN00009074",
    "ASN00009191",
    "ASN00009215",
    "ASN00009068",
    "ASN00009250",
    "ASN00009061",
    "ASN00009127",
    "ASN00009129",
    "ASN00009021",
    "ASN00009022",
    "ASN00009187",
    "ASN00009192",
    "ASN00009182",
    "ASN00009263",
    "ASN00009223",
    "ASN00009217",
    "imerg",
    "merra2",
    "era5",
    "modis"
  ],
  "city_id": "2063523"
}

```
