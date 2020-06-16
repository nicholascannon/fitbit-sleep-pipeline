#!/usr/bin/env python3
"""
Script to provision the dataware house.

Written by Nicholas Cannon
"""
import sqlalchemy as sa
from sqlalchemy_utils import create_database, database_exists


if __name__ == '__main__':
    uri = 'postgres://postgres@localhost:5432/sleep_dw'
    engine = sa.create_engine(uri, echo=True)
    if not database_exists(uri):
        create_database(uri)

    with engine.connect() as conn:
        q = """
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
        """
        conn.execute(q)
