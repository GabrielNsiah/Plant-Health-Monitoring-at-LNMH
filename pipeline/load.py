"""This is the script to load plant data into the database"""
from os import environ
from datetime import datetime
import logging
import pandas as pd
import pymssql
from dotenv import load_dotenv
load_dotenv()


def config_log() -> None:
    """Terminal logs configuration"""
    logging.basicConfig(
        format="{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M",
        level=logging.INFO,
    )


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


def get_continents(cursor) -> dict:
    """Returns a dictionart continent names and their IDs
        from the database.
        Continents are Keys and IDs are Values"""

    cursor.execute("""SELECT *
                FROM delta.Continents""")
    continent_ids = cursor.fetchall()

    continent_dict = {}

    for data in continent_ids:
        continent_dict[data[1]] = data[0]

    return continent_dict


def find_location_id(cursor, plant_data: dict[str], continent_id: str) -> int:
    """Returns the location ID as shown in the database based on data given."""
    cursor.execute("""
                SELECT location_id
                FROM delta.Locations
                WHERE latitude = %s AND longitude = %s AND town = %s
                        AND country_code = %s AND continent_id = %s AND city = %s;
            """, (plant_data["Latitude"], plant_data["Longitude"], plant_data["Town"],
                  plant_data["Country_Code"], continent_id, plant_data["City"]))
    location_id = cursor.fetchone()[0]

    return location_id


def find_botanist_id(cursor, first_name, last_name, email, phone) -> int:
    """Returns the botanist as shown in the database based on data given."""
    cursor.execute("""
                SELECT botanist_id
                FROM delta.Botanists
                WHERE first_name = %s AND last_name = %s AND email = %s AND phone = %s;
            """, (first_name, last_name, email, phone))
    botanist_id = cursor.fetchone()[0]

    return botanist_id


def find_plant_id(cursor, plant_name: str,
                  scientific_name_id: int, location_id: int, image_url: str) -> int:
    """Returns the botanist as shown in the database based on data given."""
    cursor.execute("""
            SELECT plant_id
            FROM delta.Plants
            WHERE plant_name = %s AND scientific_id = %s AND location_id = %s
                    AND image_url = %s;
        """, (plant_name, scientific_name_id, location_id, image_url))

    plant_id = cursor.fetchone()[0]
    return plant_id


def find_scientific_name_id(cursor, scientific_name: str) -> int:
    """Returns the scientific name as shown in the database based on data given."""
    cursor.execute("""
            SELECT scientific_id
            FROM delta.Scientific_Names
            WHERE scientific_name = %s;
        """, scientific_name)
    scientific_id = cursor.fetchone()[0]

    return scientific_id


def insert_botanists(cursor, plant_df: pd.DataFrame) -> None:
    """Inserts Botanist data into the relevant tables in the database
        Ignores duplicate entries"""
    for _, row in plant_df.iterrows():
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

        if not count:
            cursor.execute("""
                INSERT INTO delta.Botanists (first_name, last_name, email, phone)
                VALUES (%s, %s, %s, %s);
            """, (first_name, last_name, email, phone))
            logging.info(
                "Inserted Botanist %s %s: Email: %s, Phone: %s",
                first_name, last_name, email, phone)
        else:
            logging.info(
                "Botanist %s %s already exists with these details, Email: %s, Phone Number: %s",
                first_name, last_name, email, phone
            )


def insert_scientific_name(cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the scientific name of a plant into the database
        Ignores duplicate entries"""
    for _, row in plant_df.iterrows():
        scientific_name = row["scientific_name"]
        if pd.isna(scientific_name):
            scientific_name = "None"

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Scientific_Names
            WHERE scientific_name = %s;
        """, scientific_name)

        count = cursor.fetchone()[0]

        if not count:
            cursor.execute("""
                INSERT INTO delta.Scientific_Names (scientific_name)
                VALUES (%s);
            """, (scientific_name))
            logging.info("Inserted Scientific Name: %s", scientific_name)
        else:
            logging.info("Duplicate Scientific Name: %s", scientific_name)


def insert_location(cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the location of a plant into the database
        Ignores duplicate entries"""
    for _, row in plant_df.iterrows():
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        town = row["Town"]
        country_code = row["Country_Code"]
        continent = row["Continent"]
        city = row["City"]

        continents = get_continents(cursor)
        continent_id = continents[continent]

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Locations
            WHERE latitude = %s AND longitude = %s AND town = %s
                    AND country_code = %s AND continent_id = %s AND city = %s;
        """, (latitude, longitude, town, country_code, continent_id, city))

        count = cursor.fetchone()[0]

        if not count:
            cursor.execute("""
                INSERT INTO delta.Locations (latitude, longitude, town, country_code, continent_id, city)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (latitude, longitude, town, country_code, continent_id, city))
            logging.info(
                """Inserted Location: (Lat: %s, Long: %s, Town: %s, 
                Country Code: %s, Continent: %s, City: %s)""",
                latitude, longitude, town, country_code, continent, city
            )
        else:
            logging.info(
                """Duplicate Location: (Lat: %s, Long: %s, Town: %s, 
                Country Code: %s, Continent: %s, City: %s)""",
                latitude, longitude, town, country_code, continent, city
            )


def insert_plants(cursor, plant_df: pd.DataFrame) -> None:
    """Inserts plant information into the database
        Ignores duplicate entries"""
    for _, row in plant_df.iterrows():
        plant_id = row["plant_id"]
        plant_name = row["name"]
        image_url = row["images.original_url"]
        continent = row["Continent"]
        scientific_name = row["scientific_name"]

        continent_id = get_continents(cursor)[continent]

        if pd.isna(row["scientific_name"]):
            scientific_name = "None"
        scientific_name_id = find_scientific_name_id(
            cursor, scientific_name)

        if pd.isna(image_url):
            image_url = "None"

        location_id = find_location_id(
            cursor, row, continent_id)

        cursor.execute("""
            SELECT COUNT(*)
            FROM delta.Plants
            WHERE plant_id = %s AND plant_name = %s AND scientific_id = %s AND location_id = %s
                    AND image_url = %s;
        """, (plant_id, plant_name, scientific_name_id, location_id, image_url))

        count = cursor.fetchone()[0]

        if not count:
            cursor.execute("""
                INSERT INTO delta.Plants (plant_id, plant_name, scientific_id, location_id, image_url)
                VALUES (%s, %s, %s, %s, %s);
            """, (plant_id, plant_name, scientific_name_id, location_id, image_url))
            logging.info(
                """Inserted Plant (Plant ID: %s, Plant Name: %s, 
                Scientific Name: %s, Location ID: %s, Image URL: %s)""",
                plant_id, plant_name, scientific_name_id, location_id, image_url
            )
        else:
            logging.info(
                """Duplicate Plant (Plant ID: %s, Plant Name: %s, 
                Scientific Name: %s, Location ID: %s, Image URL: %s)""",
                plant_id, plant_name, scientific_name_id, location_id, image_url
            )


def insert_recording(cursor,  plant_df: pd.DataFrame) -> None:
    """Inserts a recording of plant status into the database."""
    for _, row in plant_df.iterrows():
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

        if not count:
            insert_query = """INSERT INTO delta.Recordings (plant_id, last_watered,
                       soil_moisture, temperature, reading_taken)
                       VALUES (%s, %s, %s, %s, %s);"""
            cursor.execute(insert_query, (plant_id, watered_datetime,
                           soil_moisture, temperature, reading_taken))
            logging.info(
                """Inserted recording: (Plant ID: %s, Last Watered: %s, 
                Soil Moisture: %s, Temperature: %s, Reading Taken: %s)""",
                plant_id, watered_datetime, soil_moisture, temperature, reading_taken
            )

        else:
            logging.info(
                """Duplicate recording: (Plant ID: %s, Last Watered: %s, 
                Soil Moisture: %s, Temperature: %s, Reading Taken: %s)""",
                plant_id, watered_datetime, soil_moisture, temperature, reading_taken
            )


def insert_assignments(cursor, plant_df: pd.DataFrame) -> None:
    """Inserts the Botanist/Plant Assignment into the database."""
    for _, row in plant_df.iterrows():
        plant_data = {
            "plant_id": row["plant_id"],
            "plant_name": row["name"],
            "scientific_name": row["scientific_name"],
            "image_url": row["images.original_url"],
            "continent": row["Continent"],
            "first_name": row["First Name"],
            "last_name": row["Last Name"],
            "email": row["botanist.email"],
            "phone": row["botanist.phone"]
        }

        botanist_id = find_botanist_id(
            cursor, plant_data["first_name"], plant_data["last_name"],
            plant_data["email"], plant_data["phone"])
        if pd.isna(plant_data["scientific_name"]):
            plant_data["scientific_name"] = "None"
        scientific_name_id = find_scientific_name_id(
            cursor, plant_data["scientific_name"])

        if pd.isna(plant_data["image_url"]):
            plant_data["image_url"] = "None"

        continents = get_continents(cursor)
        continent_id = continents[plant_data["continent"]]

        location_id = find_location_id(
            cursor, row, continent_id)
        plant_id = find_plant_id(
            cursor, plant_data["plant_name"], scientific_name_id, location_id, plant_data["image_url"])

        cursor.execute("""
                    SELECT COUNT(*)
                    FROM delta.Assignments
                    WHERE botanist_id = %s AND plant_id = %s;
                    """, (botanist_id, plant_id))
        count = cursor.fetchone()[0]

    if not count:
        cursor.execute("""
            INSERT INTO delta.Assignments (botanist_id, plant_id)
            VALUES (%s, %s);
        """, (botanist_id, plant_id))
        logging.info(
            "Registered assignment: Botanist '%s %s' (ID: %d) assigned to Plant '%s' (ID: %d)",
            plant_data["first_name"], plant_data["last_name"], botanist_id,
            plant_data["plant_name"], plant_id
        )
    else:
        logging.info(
            """Duplicate assignment detected: Botanist '%s %s' (ID: %d) 
            already assigned to Plant '%s' (ID: %d)""",
            plant_data["first_name"], plant_data["last_name"], botanist_id,
            plant_data["plant_name"], plant_id
        )


def load_data_into_database() -> None:
    """Loads data from the PLANT_DATA.csv into the database"""
    connection = get_connection()
    cursor = connection.cursor()
    plant_data = load_csv()

    insert_botanists(cursor, plant_data)
    insert_scientific_name(cursor, plant_data)
    insert_location(cursor, plant_data)
    insert_plants(cursor, plant_data)
    insert_recording(cursor, plant_data)
    insert_assignments(cursor, plant_data)


if __name__ == "__main__":
    config_log()
    load_data_into_database()
