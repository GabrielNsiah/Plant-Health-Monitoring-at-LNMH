"""This is the full ETL script to be hosted on the lambda"""
from extract import get_all_plant_data
from transform import fully_transform_data
from load import load_data_into_database
from dotenv import load_dotenv


def run_etl():
    """Runs the entire ETL pipeline in sequence"""
    load_dotenv()
    plant_data = get_all_plant_data()
    fully_transform_data(plant_data)
    load_data_into_database()


def lambda_handler(event=None, context=None):
    load_dotenv()
    load_data_into_database()
