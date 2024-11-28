"""This file is for tests relating to the load script."""
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
from dotenv import load_dotenv
from load import (
    load_csv, get_connection, get_continents, find_location_id,
    find_botanist_id, find_plant_id, find_scientific_name_id,
    insert_botanists, insert_scientific_name, insert_location,
    insert_plants, insert_recording, insert_assignments
)


MOCK_DF = pd.DataFrame({
    "First Name": "Gertrude",
    "Last Name": "Jekyll",
    "botanist.email": "gertrude.jekyll@lnhm.co.uk",
    "botanist.phone": "001-481-273-3691x127",
    "Latitude": 20.88953,
    "Longitude": -156.47432,
    "Town": "Kahului",
    "Country_Code": "US",
    "Continent": "Pacific",
    "City": "Honolulu",
    "scientific_name": "Asclepias curassavica",
    "name": "Asclepias Curassavica",
    "images.original_url": """https://perenual.com/storage/
    species_image/1007_asclepias_curassavica/og/51757177616_7ca0baaa87_b.jpg""",
    "plant_id": 1,
    "last_watered": "Wed, 27 Nov 2024 13:37:24 GMT",
    "soil_moisture": 89.2981898174157,
    "temperature": 9.51395806835785,
    "recording_taken": "2024-11-27 16:47:53"
}, index=[0])


@pytest.fixture(name='mock_cursor')
def mock_cursor_fixture():
    """Mock database cursor fixture"""
    cursor = MagicMock()
    cursor.fetchone.side_effect = lambda: [
        1]
    cursor.fetchall.side_effect = lambda: [(1, "Pacific")]
    return cursor


@patch("load.pd.read_csv")
def test_load_csv(mock_read_csv):
    """Test the load_csv function"""
    mock_read_csv.return_value = MOCK_DF
    result = load_csv()
    pd.testing.assert_frame_equal(result, MOCK_DF)
    mock_read_csv.assert_called_once_with("PLANT_DATA.csv")


@patch("load.pymssql.connect")
def test_get_connection(mock_connect):
    """Test the get_connection function"""
    load_dotenv()
    mock_connect.return_value = "MockConnection"
    connection = get_connection()
    assert connection == "MockConnection"
    mock_connect.assert_called_once()


def test_get_continents(mock_cursor):
    """Test the get_continents function"""
    continents = get_continents(mock_cursor)
    assert continents == {"Pacific": 1}
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM delta.Continents")


def test_find_location_id(mock_cursor):
    """Test the find_location_id function"""
    plant_data = {
        "Latitude": 20.88953,
        "Longitude": -156.47432,
        "Town": "Kahului",
        "Country_Code": "US",
        "City": "Honolulu"
    }
    location_id = find_location_id(mock_cursor, plant_data, 1)
    assert location_id == 1
    mock_cursor.execute.assert_called_once()


def test_find_botanist_id(mock_cursor):
    """Test the find_botanist_id function"""
    botanist_id = find_botanist_id(
        mock_cursor, "Gertrude", "Jekyll", "gertrude.jekyll@lnhm.co.uk", "001-481-273-3691x127")
    assert botanist_id == 1
    mock_cursor.execute.assert_called_once()


def test_find_plant_id(mock_cursor):
    """Test the find_plant_id function"""
    plant_id = find_plant_id(mock_cursor, "Asclepias Curassavica",
                             1, 1, """https://perenual.com/storage/
    species_image/1007_asclepias_curassavica/og/51757177616_7ca0baaa87_b.jpg""")
    assert plant_id == 1
    mock_cursor.execute.assert_called_once()


def test_find_scientific_name_id(mock_cursor):
    """Test the find_scientific_name_id function"""
    scientific_id = find_scientific_name_id(
        mock_cursor, "Asclepias curassavica")
    assert scientific_id == 1
    mock_cursor.execute.assert_called_once()


@patch("load.get_continents")
def test_insert_location(mock_get_continents, mock_cursor):
    """Test the insert_location function"""
    mock_get_continents.return_value = {"Pacific": 1}
    insert_location(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()


@patch("load.get_continents")
@patch("load.find_scientific_name_id")
@patch("load.find_location_id")
def test_insert_plants(mock_find_location_id, mock_find_scientific_name_id,
                       mock_get_continents, mock_cursor):
    """Test the insert_plants function"""
    mock_get_continents.return_value = {"Pacific": 1}
    mock_find_scientific_name_id.return_value = 1
    mock_find_location_id.return_value = 1
    insert_plants(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()


def test_insert_botanists(mock_cursor):
    """Test the insert_botanists function"""
    insert_botanists(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()


def test_insert_scientific_name(mock_cursor):
    """Test the insert_scientific_name function"""
    insert_scientific_name(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()


def test_insert_recording(mock_cursor):
    """Test the insert_recording function"""
    insert_recording(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()


@patch("load.find_botanist_id")
@patch("load.find_plant_id")
def test_insert_assignments(mock_find_plant_id, mock_find_botanist_id, mock_cursor):
    """Test the insert_assignments function"""
    mock_find_plant_id.return_value = 1
    mock_find_botanist_id.return_value = 1
    insert_assignments(mock_cursor, MOCK_DF)
    mock_cursor.execute.assert_called()
