# pylint: disable=no-member
"""Lambda script for moving data rows older than 24 hours from
short-term RDS storage to long-term S3 bucket storage."""

from os import environ
from datetime import datetime
import boto3
import pymssql
from dotenv import load_dotenv
import pandas as pd


def convert_data_to_df(recording_data: list[dict]) -> pd.DataFrame:
    """Converts data queried from RDS into a pandas df."""
    recordings_df = pd.DataFrame(columns=['recording_id', 'plant_id', 'last_watered',
                                          'soil_moisture', 'temperature', 'recording_taken'])
    rows_added = 0

    for row in recording_data:
        last_watered = row['last_watered'].strftime("%Y-%m-%d %H:%M:%S")
        reading_taken = row['reading_taken'].strftime("%Y-%m-%d %H:%M:%S")

        recordings_df.loc[rows_added] = [row['recording_id'], row['plant_id'], last_watered,
                                         row['soil_moisture'], row['temperature'], reading_taken]

        rows_added += 1

    return recordings_df


def download_csv_from_s3(s3: boto3.client) -> bool:
    """Attempts to download .csv file from S3 bucket."""
    filename = 'updated_recordings_data.csv'

    try:
        for object_name in s3.list_objects(Bucket='c14-gbu-storage')['Contents']:
            object_key = object_name['Key']

            if object_key == filename:
                s3.download_file('c14-gbu-storage', object_key,
                                 'existing_recordings.csv')
                return True
    except KeyError:
        return False

    return False


def merge_with_existing_recordings(recordings_df: pd.DataFrame, s3: boto3.client,
                                   test_mode=False) -> pd.DataFrame:
    """Merges df from RDS query with existing df from S3 bucket, sorting by
    newest to oldest recording_taken."""
    file_found = download_csv_from_s3(s3)
    if test_mode is False:
        csv_filename = "existing_recordings.csv"
    else:
        csv_filename = "test_existing_recordings.csv"

    if file_found is True:
        existing_recordings_df = pd.read_csv(f'./{csv_filename}')
    else:
        existing_recordings_df = pd.DataFrame(columns=['recording_id', 'plant_id',
                                                       'last_watered', 'soil_moisture',
                                                       'temperature', 'recording_taken'])

    updated_recordings_df = pd.concat(
        [recordings_df, existing_recordings_df], ignore_index=True)

    updated_recordings_df['recording_taken'] = pd.to_datetime(
        updated_recordings_df['recording_taken'])
    sorted_updated_df = updated_recordings_df.sort_values(
        by='recording_taken', ascending=False)

    print("\n\n")
    print(sorted_updated_df)

    if test_mode is False:
        sorted_updated_df.to_csv(
            'updated_recordings_data.csv', index=False)

    return sorted_updated_df


def update_csv_on_s3(s3):
    """Uploads the updated .csv file to the S3 bucket."""
    s3.upload_file('updated_recordings_data.csv',
                   'c14-gbu-storage', 'updated_recordings_data.csv')


def query_database(conn: pymssql.Connection, testing_mode=False) -> list[dict]:
    """Queries the RDS for any rows with recording_taken value
    older than 24 hours, removing and returning those rows."""
    if testing_mode is False:
        query_table = "Recordings"
    else:
        query_table = "Test_Recordings"

    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT * FROM delta.{query_table} 
            WHERE reading_taken < DATEADD(HOUR, -24, SYSDATETIME()) 
            ORDER BY reading_taken DESC;""")
        recording_data = cur.fetchall()

        cur.execute(
            f"""DELETE FROM delta.{query_table} 
            WHERE reading_taken < DATEADD(HOUR, -24, SYSDATETIME());""")

    conn.commit()

    return recording_data


def lambda_handler(event=None, context=None) -> None:
    """Equivalent to __main__ function, for running script
    on AWS Lambda function."""
    load_dotenv()

    conn = pymssql.connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"],
        as_dict=True
    )

    recording_data = query_database(conn)

    if recording_data == []:
        print("No recordings older than 24 hours, 0 rows removed.")
        return

    print("The following recordings have been removed from the database:\n")
    print(recording_data)

    recordings_df = convert_data_to_df(recording_data)
    s3 = boto3.client('s3', aws_access_key_id=environ.get(
        "aws_access_key_id"), aws_secret_access_key=environ.get("aws_secret_access_key"))

    updated_df = merge_with_existing_recordings(recordings_df, s3)
    update_csv_on_s3(s3)


if __name__ == "__main__":

    lambda_handler()
