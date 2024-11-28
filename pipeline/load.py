"""This is the script to load plant data into the database"""
from os import environ
import pandas as pd
from datetime import datetime
import pymssql
from dotenv import load_dotenv

load_dotenv()


def load_csv() -> pd.DataFrame:
    """Loads a given csv into a dataframe"""
    plant_data = pd.read_csv("PLANT_DATA.csv")
    return plant_data


def get_connection() -> None:
    """Returns a connection to the database"""
    connection = pymssql.connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"]
    )

    return connection


def find_location_id(cursor: pymssql.Cursor, latitude: int, longitude: int, town: str, country_code: str,
                     continent_id: str, city: str) -> int:
    """Returns the location ID as shown in the database based on data given."""
    cursor.execute("""
                SELECT location_id
                FROM delta.Locations
                WHERE latitude = %s AND longitude = %s AND town = %s
                        AND country_code = %s AND continent_id = %s AND city = %s;
            """, (latitude, longitude, town, country_code, continent_id, city))
    location_id = cursor.fetchone()[0]

    return location_id


def find_botanist_id(cursor: pymssql.Cursor, first_name, last_name, email, phone) -> int:
    """Returns the botanist as shown in the database based on data given."""
    cursor.execute("""
                SELECT botanist_id
                FROM delta.Botanists
                WHERE first_name = %s AND last_name = %s AND email = %s AND phone = %s;
            """, (first_name, last_name, email, phone))
    botanist_id = cursor.fetchone()[0]

    return botanist_id


def find_plant_id(cursor: pymssql.Cursor, plant_name, scientific_name_id, location_id, image_url) -> int:
    """Returns the botanist as shown in the database based on data given."""
    cursor.execute("""
            SELECT plant_id
            FROM delta.Plants
            WHERE plant_name = %s AND scientific_id = %s AND location_id = %s
                    AND image_url = %s;
        """, (plant_name, scientific_name_id, location_id, image_url))

    plant_id = cursor.fetchone()[0]
    return plant_id


def find_scientific_name_id(cursor: pymssql.Cursor, scientific_name: str) -> int:
    cursor.execute("""
            SELECT scientific_id
            FROM delta.Scientific_Names
            WHERE scientific_name = %s;
        """, scientific_name)
    scientific_id = cursor.fetchone()[0]

    return scientific_id


def insert_botanists(cursor: pymssql.Cursor, plant_df: pd.DataFrame) -> None:
    """Inserts Botanist data into the relevant tables in the database
        Ignores duplicate entries"""
    for index, row in plant_df.iterrows():
        first_name = row["First Name"]
        last_name = row["Last Name"]
        email = row["botanist.email"]
        phone = row["botanist.phone"]

        # Checks the database for duplicate information.
        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Botanists
            WHERE first_name = %s AND last_name = %s AND email = %s AND phone = %s;
        """, (first_name, last_name, email, phone))

        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                INSERT INTO delta.Botanists (first_name, last_name, email, phone)
                VALUES (%s, %s, %s, %s);
            """, (first_name, last_name, email, phone))
            print(f"Inserted: {first_name} {last_name}, {email}, {phone}")
        else:
            print(f"""Duplicate found: {first_name} {
                    last_name}, {email}, {phone}""")


def insert_scientific_name(cursor: pymssql.Cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the scientific name of a plant into the database
        Ignores duplicate entries"""
    for index, row in plant_df.iterrows():
        scientific_name = row["scientific_name"]
        if pd.isna(scientific_name):
            scientific_name = "None"
        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Scientific_Names
            WHERE scientific_name = %s;
        """, scientific_name)

        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                INSERT INTO delta.Scientific_Names (scientific_name)
                VALUES (%s);
            """, (scientific_name))
            print(f"Inserted: {scientific_name}")
        else:
            print(f"Duplicate: {scientific_name}")


def insert_location(cursor: pymssql.Cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the location of a plant into the database
        Ignores duplicate entries"""
    for index, row in plant_df.iterrows():
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        town = row["Town"]
        country_code = row["Country_Code"]
        continent = row["Continent"]
        city = row["City"]

        cursor.execute("""SELECT continent_id
                    FROM delta.Continents
                    WHERE continent_name = %s;
                    """, (continent))
        continent_id = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Locations
            WHERE latitude = %s AND longitude = %s AND town = %s
                    AND country_code = %s AND continent_id = %s AND city = %s;
        """, (latitude, longitude, town, country_code, continent_id, city))

        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                INSERT INTO delta.Locations (latitude, longitude, town, country_code, continent_id, city)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (latitude, longitude, town, country_code, continent_id, city))
            print(
                f"Inserted: ({latitude}, {longitude}, {town}, {country_code}, {continent_id}, {city})")
        else:
            print(f"""Duplicate: {latitude}, {longitude}, {
                    town}, {country_code}, {continent_id}, {city}""")


def insert_plants(cursor: pymssql.Cursor, plant_df: pd.DataFrame) -> None:
    """Inserts plant information into the database
        Ignores duplicate entries"""
    for index, row in plant_df.iterrows():
        plant_id = row["plant_id"]
        plant_name = row["name"]
        scientific_name = row["scientific_name"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        image_url = row["images.original_url"]
        town = row["Town"]
        country_code = row["Country_Code"]
        continent = row["Continent"]
        city = row["City"]

        try:
            cursor.execute("""SELECT continent_id
                        FROM delta.Continents
                        WHERE continent_name = %s;
                        """, (continent))
            continent_id = cursor.fetchone()[0]
        except:
            continue
        if pd.isna(scientific_name):
            scientific_name = "None"
        scientific_name_id = find_scientific_name_id(
            cursor, scientific_name)

        if pd.isna(image_url):
            image_url = "None"

        location_id = find_location_id(
            cursor, latitude, longitude, town, country_code, continent_id, city)

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Plants
            WHERE plant_id = %s AND plant_name = %s AND scientific_id = %s AND location_id = %s
                    AND image_url = %s;
        """, (plant_id, plant_name, scientific_name_id, location_id, image_url))

        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                INSERT INTO delta.Plants (plant_id, plant_name, scientific_id, location_id, image_url)
                VALUES (%s, %s, %s, %s, %s);
            """, (plant_id, plant_name, scientific_name_id, location_id, image_url))
            print(
                f"Inserted: ({plant_id}, {plant_name}, {scientific_name_id}, {location_id}, {image_url})")
        else:
            print(f"""Duplicate: ({plant_id}, {plant_name}, {
                scientific_name_id}, {location_id}, {image_url})""")


def insert_recording(cursor: pymssql.Cursor,  plant_df: pd.DataFrame) -> None:
    for index, row in plant_df.iterrows():
        plant_id = row["plant_id"]
        last_watered = row["last_watered"]
        soil_moisture = row["soil_moisture"]
        temperature = row["temperature"]
        reading_taken = row["recording_taken"]

        watered_datetime = datetime.strptime(
            last_watered, "%a, %d %b %Y %H:%M:%S %Z")

        recording_datetime = datetime.strptime(
            reading_taken, "%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Recordings
            WHERE plant_id = %s AND last_watered = %s AND soil_moisture = %s AND temperature = %s
                    AND reading_taken = %s;
        """, (plant_id, watered_datetime, soil_moisture, temperature, recording_datetime))

        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                        INSERT INTO delta.Recordings (plant_id, last_watered, soil_moisture, temperature, reading_taken)
                        VALUES (%s, %s, %s, %s, %s);""", (plant_id, watered_datetime, soil_moisture, temperature, reading_taken))
            print(
                f"Inserted: ({plant_id}, {watered_datetime}, {soil_moisture}, {temperature}, {reading_taken})")
        else:
            print(f"""Duplicate: ({plant_id}, {watered_datetime}, {
                    soil_moisture}, {temperature}, {reading_taken})""")


def insert_assignments(cursor: pymssql.Cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the Botanist/Plant Assignment into the database."""
    for index, row in plant_df.iterrows():
        plant_id = row["plant_id"]
        plant_name = row["name"]
        scientific_name = row["scientific_name"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        image_url = row["images.original_url"]
        town = row["Town"]
        country_code = row["Country_Code"]
        continent = row["Continent"]
        city = row["City"]
        first_name = row["First Name"]
        last_name = row["Last Name"]
        email = row["botanist.email"]
        phone = row["botanist.phone"]

        botanist_id = find_botanist_id(
            cursor, first_name, last_name, email, phone)
        if pd.isna(scientific_name):
            scientific_name = "None"
        scientific_name_id = find_scientific_name_id(
            cursor, scientific_name)

        if pd.isna(image_url):
            image_url = "None"
        try:
            cursor.execute("""SELECT continent_id
                            FROM delta.Continents
                            WHERE continent_name = %s;
                            """, (continent))
            continent_id = cursor.fetchone()[0]
        except:
            continue
        location_id = find_location_id(
            cursor, latitude, longitude, town, country_code, continent_id, city)
        plant_id = find_plant_id(
            cursor, plant_name, scientific_name_id, location_id, image_url)

        cursor.execute("""
                    SELECT COUNT(*)
                    FROM delta.Assignments
                    WHERE botanist_id = %s AND plant_id = %s;
                    """, (botanist_id, plant_id))
        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("""
                        INSERT INTO delta.Assignments (botanist_id, plant_id)
                        VALUES (%s, %s);""", (botanist_id, plant_id))
            print(f"""Registered assignment of {first_name} {
                    last_name} {botanist_id} to {plant_name} {plant_id}""")
        else:
            print(f"""Duplicate assignment of {first_name} {
                    last_name} {botanist_id} to {plant_name} {plant_id} """)


if __name__ == "__main__":
    connection = get_connection()
    cursor = connection.cursor()
    plant_data = load_csv()
    insert_botanists(cursor, plant_data)
    insert_scientific_name(cursor, plant_data)
    insert_location(cursor, plant_data)
    insert_plants(cursor, plant_data)
    insert_recording(cursor, plant_data)
    insert_assignments(cursor, plant_data)
