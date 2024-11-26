"""This is the script for the Extract portion of the ETL pipeline."""
import requests


def function_to_test():
    """A function to test GitHub Actions"""
    return True


def fetch_api_plant_data(plant_id: int) -> dict:
    """Gets plant data for a plant with given id, 
    returning the data as a dictionary."""
    return


def get_all_plant_data() -> list[dict]:
    """Returns all plant data for all 50 plants as
    a list of dictionaries."""
    return


if __name__ == "__main__":
    response = requests.get(
        "https://data-eng-plants-api.herokuapp.com/plants/1", timeout=100)

    print(response.json())
