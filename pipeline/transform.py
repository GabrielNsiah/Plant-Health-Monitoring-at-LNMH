"""This is the Transform portion of the ETL script"""
import pandas as pd
import requests


def insert_in_dataframe(plant_data: list[dict]) -> pd.DataFrame:
    """Creates a dataframe for plant data
       Inserts plant data for each plant inside
       Returns the dataframe."""

    plant_df = pd.json_normalize(plant_data)

    location_df = plant_df['origin_location'].apply(pd.Series)

    location_df.columns = ['Latitude', 'Longitude',
                           'Town', 'Country_Code', 'Continent_City']

    location_df[['Continent', 'City']
                ] = location_df['Continent_City'].str.split('/', expand=True)

    location_df = location_df.drop(columns=['Continent_City'])

    plant_df = plant_df.drop(columns=['origin_location'])

    plant_df = pd.concat([plant_df, location_df], axis=1)

    plant_df['scientific_name'] = plant_df['scientific_name'].apply(
        lambda x: x[0] if isinstance(x, list) else x)

    plant_df[["First Name", "Last Name"]
             ] = plant_df["botanist.name"].str.split(" ", n=1, expand=True)

    return plant_df


def clean_data(plant_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans dataframe of unnecessary data
        Removes unwanted columns"""

    plant_df["City"] = plant_df["City"].str.replace("_", " ")

    columns_to_drop = ["images.license", "images.license_name", "images.license_url",
                       "images.medium_url", "images.small_url", "images.thumbnail",
                       "images.regular_url", "botanist.name"]

    plant_df = plant_df.where(pd.notnull(plant_df), "None")

    plant_df = plant_df.drop(columns=columns_to_drop)

    return plant_df


def export_as_csv(plant_df: pd.DataFrame) -> None:
    """Exports the dataframe as a csv file."""
    plant_df.to_csv("PLANT_DATA.csv", index=False)


def fully_transform_data(plant_data: list[dict]) -> None:
    """Fully transforms the given plant data and also
        Exports it to a csv file"""
    plant_df = insert_in_dataframe(plant_data)
    plant_df = clean_data(plant_df)
    export_as_csv(plant_df)


if __name__ == "__main__":
    plant_data = get_all_plant_data()
    fully_transform_data(plant_data)
    pass
