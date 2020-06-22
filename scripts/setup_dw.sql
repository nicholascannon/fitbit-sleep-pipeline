-- Setup Data Warehouse
CREATE DATABASE sleep_dw;
DROP TABLE IF EXISTS daily_sleep_data;
CREATE TABLE daily_sleep_data (
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