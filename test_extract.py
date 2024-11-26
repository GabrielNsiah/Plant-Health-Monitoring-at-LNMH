"""This file is for tests relating to the extract script."""
import unittest
from unittest.mock import patch
import requests
from extract import function_to_test, fetch_api_plant_data, get_all_plant_data


def test_working_function():
    """A test function to allow GitHub Actions to complete"""
    assert function_to_test() is True


@patch("extract.requests.get")
def test_fetch_api_plant_data_valid(mock_get):
    """Test fetch_api_plant_data with valid data"""
    mock_response = {
        "botanist": {
            "email": "eliza.andrews@lnhm.co.uk",
            "name": "Eliza Andrews",
            "phone": "(846)669-6651x75948"
        },
        "name": "Bird of paradise",
        "plant_id": 8,
    }
    mock_get.return_value.json.return_value = mock_response

    result = fetch_api_plant_data(8)
    assert result["name"] == "Bird of paradise"
    assert result["plant_id"] == 8
    assert result["botanist"]["name"] == "Eliza Andrews"


@patch("extract.requests.get")
def test_fetch_api_plant_data_invalid(mock_get):
    """Test fetch_api_plant_data with invalid data"""
    mock_response = {"error": "plant not found"}
    mock_get.return_value.json.return_value = mock_response

    result = fetch_api_plant_data(100)
    assert "error" in result
    assert result["error"] == "plant not found"


@patch("extract.fetch_api_plant_data")
def test_get_all_plant_data(mock_fetch):
    """Test get_all_plant_data with a mix of valid and invalid responses"""
    mock_fetch.side_effect = [
        {"name": "Cordyline Fruticosa", "plant_id": 1},
        {"error": "plant not found"},
        {"name": "Euphorbia Cotinifolia", "plant_id": 2},
    ] + [{"error": "plant not found"}] * 48

    result = get_all_plant_data()
    assert len(result) == 2
    assert result[0]["name"] == "Cordyline Fruticosa"
    assert result[1]["name"] == "Euphorbia Cotinifolia"


@patch("extract.requests.get")
def test_fetch_api_plant_data_timeout(mock_get):
    """Test fetch_api_plant_data handles timeout"""
    mock_get.side_effect = requests.exceptions.Timeout
    try:
        fetch_api_plant_data(8)
    except requests.exceptions.Timeout:
        assert True
    else:
        assert False, "Timeout exception not raised"


if __name__ == "__main__":
    unittest.main()
