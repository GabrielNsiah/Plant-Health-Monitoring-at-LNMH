"""This file is for tests relating to the transform script."""
from unittest.mock import patch
import pytest
import pandas as pd
from transform import insert_in_dataframe, clean_data, fully_transform_data


@pytest.fixture(name='sample_plant_data')
def plant_data():
    """Sample plant data fixture"""
    return [
        {
            "botanist": {
                "email": "eliza.andrews@lnhm.co.uk",
                "name": "Eliza Andrews",
                "phone": "(846)669-6651x75948"
            },
            "images": {
                "license": 45,
                "license_name": "Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)",
                "license_url": "https://creativecommons.org/licenses/by-sa/3.0/deed.en",
                "medium_url": """https://perenual.com/storage/species_image/
                2045_cordyline_fruticosa/medium/2560px-Cordyline_fruticosa_Rubra_1.jpg""",
                "original_url": """https://perenual.com/storage/species_image/
                2045_cordyline_fruticosa/og/2560px-Cordyline_fruticosa_Rubra_1.jpg""",
                "regular_url": """https://perenual.com/storage/species_image/
                2045_cordyline_fruticosa/regular/2560px-Cordyline_fruticosa_Rubra_1.jpg""",
                "small_url": """https://perenual.com/storage/species_image/
                2045_cordyline_fruticosa/small/2560px-Cordyline_fruticosa_Rubra_1.jpg""",
                "thumbnail": """https://perenual.com/storage/species_image/
                2045_cordyline_fruticosa/thumbnail/2560px-Cordyline_fruticosa_Rubra_1.jpg"""
            },
            "last_watered": "Mon, 25 Nov 2024 14:56:47 GMT",
            "name": "Cordyline Fruticosa",
            "origin_location": [
                "52.53048",
                "13.29371",
                "Charlottenburg-Nord",
                "DE",
                "Europe/Berlin"
            ],
            "plant_id": 23,
            "recording_taken": "2024-11-26 13:55:35",
            "scientific_name": [
                "Cordyline fruticosa"
            ],
            "soil_moisture": 20.6058731689059,
            "temperature": 11.4972106079503
        }
    ]


@pytest.fixture(name='sample_dataframe')
def plane_sample_dataframe(sample_plant_data):
    """Sample dataframe created from sample plant data"""
    return insert_in_dataframe(sample_plant_data)


def test_insert_in_dataframe(sample_plant_data):
    """Test insert_in_dataframe function"""
    sample_data = insert_in_dataframe(sample_plant_data)

    assert not sample_data.empty

    expected_columns = [
        "scientific_name", "Latitude", "Longitude", "Town",
        "Country_Code", "Continent", "City", "First Name", "Last Name"
    ]
    assert all(column in sample_data.columns for column in expected_columns)

    assert sample_data.loc[0, "scientific_name"] == "Cordyline fruticosa"
    assert sample_data.loc[0, "Latitude"] == "52.53048"
    assert sample_data.loc[0, "City"] == "Berlin"


def test_clean_data(sample_dataframe):
    """Test clean_data function"""
    cleaned_data = clean_data(sample_dataframe)

    unwanted_columns = [
        "images.license", "images.license_name", "images.license_url",
        "images.medium_url", "images.small_url", "images.thumbnail",
        "images.regular_url", "botanist.name"
    ]
    for column in unwanted_columns:
        assert column not in cleaned_data.columns

    assert cleaned_data.loc[0, "City"] == "Berlin"
    assert "None" not in cleaned_data.values


def test_export_as_csv(sample_dataframe, tmp_path):
    """Test export_as_csv function"""
    test_file = tmp_path / "test_plant_data.csv"
    sample_dataframe.to_csv(test_file, index=False)

    assert test_file.exists()

    exported_data = pd.read_csv(test_file)
    assert not exported_data.empty
    assert "scientific_name" in exported_data.columns


def test_main(sample_plant_data):
    """Test the main function and see if the export csv is called and populated"""
    with patch("transform.export_as_csv") as mock_export:
        fully_transform_data(sample_plant_data)

        mock_export.assert_called_once()
        exported_data = mock_export.call_args[0][0]

        assert not exported_data.empty
        assert "scientific_name" in exported_data.columns
        assert exported_data.loc[0, "scientific_name"] == "Cordyline fruticosa"
        assert exported_data.loc[0, "City"] == "Berlin"
