# pylint: disable=no-member
"""Test file using integrated testing for lambda_mover.py"""

from datetime import datetime
from os import environ
from unittest.mock import patch
from dotenv import load_dotenv
import pytest
import pymssql
from lambda_mover import convert_data_to_df, merge_with_existing_recordings, query_database

load_dotenv()

conn = pymssql.connect(
    server=environ["DB_HOST"],
    port=environ["DB_PORT"],
    user=environ["DB_USER"],
    password=environ["DB_PASSWORD"],
    database=environ["DB_NAME"],
    as_dict=True
)


@pytest.fixture(name='test_recording_1')
def test_recording_data1():
    return (10, "2024-11-27 14:35:45",
            40.22, 23.1, "2024-11-25 16:35:45")


@pytest.fixture(name='test_recording_2')
def test_recording_data2():
    return (10, "2024-11-27 14:35:45",
            40.22, 23.1, "2024-11-26 13:31:27")


@pytest.fixture(name='test_recording_3')
def test_recording_data3():
    return (10, "2024-11-27 14:35:45",
            40.22, 23.1, datetime.today().strftime("%Y-%m-%d %H:%M:%S"))


def test_drop_test_table():
    """Drops test table on RDS"""
    with conn.cursor() as cur:
        cur.execute("""
IF OBJECT_ID('delta.Test_Recordings', 'U') IS NOT NULL
    DROP TABLE delta.Test_Recordings;""")

    conn.commit()


def test_set_up_test_table():
    """Sets up empty test table on RDS"""
    with conn.cursor() as cur:
        cur.execute("""
CREATE TABLE delta.Test_Recordings (
    recording_id INT IDENTITY (1, 1) PRIMARY KEY,
    plant_id INT NOT NULL,
    last_watered datetime2,
    soil_moisture FLOAT NOT NULL,
    temperature FLOAT NOT NULL,
    reading_taken datetime2 NOT NULL
);""")

    conn.commit()


def test_input_test_data_into_rds(test_recording_1, test_recording_2, test_recording_3):
    """Inputs test data into test table on RDS"""
    test_recordings = [test_recording_1, test_recording_2, test_recording_3]
    with conn.cursor() as cur:
        for test_recording in test_recordings:
            cur.execute("""
            INSERT INTO delta.Test_Recordings (plant_id, last_watered, soil_moisture, temperature, reading_taken)
            VALUES (%s, %s, %s, %s, %s);""", test_recording)

            conn.commit()


def test_query_database():
    """Test the query_database function to ensure SQL queries
    returning what is expected, i.e. entries older than 24 hours
    in order of newest to oldest."""
    test_data_returned = query_database(conn, True)
    recording_ids = []
    reading_taken = []
    for test_data in test_data_returned:
        recording_ids.append(test_data['recording_id'])
        reading_taken.append(test_data['reading_taken'])

    assert recording_ids == [2, 1]
    assert reading_taken[0] > reading_taken[1]


@patch("lambda_mover.download_csv_from_s3")
def test_merge_with_existing_recordings(mock_download_csv, test_recording_1,
                                        test_recording_2, test_recording_3):
    """tests the merge_with_existing_recordings function to
    ensure the merged pandas df is the combined length of
    the test csv file and test df."""
    mock_download_csv.return_value = False
    test_input_test_data_into_rds(
        test_recording_1, test_recording_2, test_recording_3)
    test_recordings = query_database(conn, True)

    test_recordings_df = convert_data_to_df(test_recordings)

    test_updated_df = merge_with_existing_recordings(
        test_recordings_df, 's3', True)

    assert len(test_recordings_df) == 2
    assert len(test_updated_df) == 4


test_drop_test_table()
