"""This is the script for the Extract portion of the ETL pipeline."""
import requests


def function_to_test():
    """A function to test GitHub Actions"""
    return True


def fetch_api_plant_data(plant_id: int) -> dict:
    """Gets plant data for a plant with given id,
    returning the data as a dictionary."""
    response = requests.get(
        f"https://data-eng-plants-api.herokuapp.com/plants/{str(plant_id)}", timeout=100)
    return response.json()


def get_all_plant_data() -> list[dict]:
    """Returns all plant data for all 50 plants as
    a list of dictionaries."""
    data_list = []
    for i in range(0, 51):
        plant_data = fetch_api_plant_data(i)
        if "error" in plant_data.keys():
            continue
        data_list.append(plant_data)
    return data_list


if __name__ == "__main__":
    pass
