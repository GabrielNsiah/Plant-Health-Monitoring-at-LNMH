# pylint: disable=redefined-outer-name, no-member
"""Base script for making merged dataframe from short
and long-term AWS storages."""

from os import environ
from datetime import datetime
import boto3
import pymssql
from dotenv import load_dotenv
import pandas as pd


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


def query_database(conn: pymssql.Connection) -> list[dict]:
    """Fetches all short-term data from the RDS, returning in order
    of newest to oldest reading_taken."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT * FROM delta.Recordings ORDER BY reading_taken DESC;""")
        recording_data = cur.fetchall()

    conn.commit()

    return recording_data


def merge_with_existing_recordings(recordings_df: pd.DataFrame) -> pd.DataFrame:
    """Merges RDS df with S3 bucket df to form one df in order
    of newest to oldest reading_taken."""
    long_term_df = pd.read_csv('./existing_recordings.csv')

    merged_recordings_df = pd.concat(
        [recordings_df, long_term_df], ignore_index=True)

    merged_recordings_df['recording_taken'] = pd.to_datetime(
        merged_recordings_df['recording_taken'])

    sorted_merged_df = merged_recordings_df.sort_values(
        by='recording_taken', ascending=False)

    return sorted_merged_df


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


if __name__ == "__main__":
    load_dotenv()

    conn = pymssql.connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"],
        as_dict=True
    )

    rds_data = query_database(conn)
    rds_df = convert_data_to_df(rds_data)

    s3 = boto3.client('s3', aws_access_key_id=environ.get(
        "aws_access_key_id"), aws_secret_access_key=environ.get("aws_secret_access_key"))

    download_csv_from_s3(s3)

    merged_df = merge_with_existing_recordings(rds_df)

    print(merged_df)
