"""This is the Transform portion of the ETL script"""
import pandas as pd


def insert_in_dataframe(plant_data: list[dict]) -> pd.DataFrame:
    """Creates a dataframe for plant data
        Inserts plant data for each plant inside
        Returns the dataframe."""

    return plant_data


def clean_data(plant_df: pd.DataFrame):
    """Cleans dataframe of erroneous/extreme values"""

    return plant_df


def export_as_csv(plant_df: pd.DataFrame):
    """Exports the dataframe as a csv file."""

    return plant_df


if __name__ == "__main__":
    pass
