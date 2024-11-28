"""This is the script to load plant data into the database"""
from os import environ
import pandas as pd
from datetime import datetime
from pymssql import connect
from dotenv import load_dotenv

load_dotenv()


def load_csv() -> pd.DataFrame:
    """Loads a given csv into a dataframe"""
    plant_data = pd.read_csv("PLANT_DATA.csv")
    return plant_data


def get_connection() -> None:
    """Returns a connection to the database"""
    conn = connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"]
    )

    return conn


def find_location_id(conn: 'connection', latitude: int, longitude: int, town: str, country_code: str,
                     continent_id: str, city: str) -> int:
    """Finds the location ID as shown in the database based on data given."""
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT location_id
                    FROM delta.Locations
                    WHERE latitude = %s AND longitude = %s AND town = %s
                            AND country_code = %s AND continent_id = %s AND city = %s;
                """, (latitude, longitude, town, country_code, continent_id, city))
        location_id = cur.fetchone()[0]

        return location_id


def find_botanist_id(conn: 'connection', first_name, last_name, email, phone) -> int:
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT botanist_id
                    FROM delta.Botanists
                    WHERE first_name = %s AND last_name = %s AND email = %s AND phone = %s;
                """, (first_name, last_name, email, phone))
        botanist_id = cur.fetchone()[0]

        return botanist_id


def find_plant_id(conn: 'connection', plant_name, scientific_name_id, location_id, image_url) -> int:
    with conn.cursor() as cur:
        cur.execute("""
                SELECT plant_id
                FROM delta.Plants
                WHERE plant_name = %s AND scientific_id = %s AND location_id = %s
                        AND image_url = %s;
            """, (plant_name, scientific_name_id, location_id, image_url))

        plant_id = cur.fetchone()[0]
        return plant_id


def find_scientific_name_id(conn: 'connection', scientific_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
                SELECT scientific_id
                FROM delta.Scientific_Names
                WHERE scientific_name = %s;
            """, scientific_name)
        scientific_id = cur.fetchone()[0]

        return scientific_id


def insert_botanists(conn: 'connection', plant_df: pd.DataFrame) -> None:
    """Inserts Botanist data into the relevant tables in the database
        Ignores duplicate entries"""
    with conn.cursor() as cur:
        for index, row in plant_df.iterrows():
            first_name = row["First Name"]
            last_name = row["Last Name"]
            email = row["botanist.email"]
            phone = row["botanist.phone"]

            cur.execute("""
                SELECT COUNT(*)
                FROM delta.Botanists
                WHERE first_name = %s AND last_name = %s AND email = %s AND phone = %s;
            """, (first_name, last_name, email, phone))

            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                    INSERT INTO delta.Botanists (first_name, last_name, email, phone)
                    VALUES (%s, %s, %s, %s);
                """, (first_name, last_name, email, phone))
                print(f"Inserted: {first_name} {last_name}, {email}, {phone}")
            else:
                print(f"""Duplicate found: {first_name} {
                      last_name}, {email}, {phone}""")


def insert_scientific_name(conn: 'connection', plant_df: pd.DataFrame) -> None:
    """Inserts the scientific name of a plant into the database
        Ignores duplicate entries"""
    with conn.cursor() as cur:
        for index, row in plant_df.iterrows():
            scientific_name = row["scientific_name"]
            if pd.isna(scientific_name):
                scientific_name = "None"
            cur.execute("""
                SELECT COUNT(*)
                FROM delta.Scientific_Names
                WHERE scientific_name = %s;
            """, scientific_name)

            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                    INSERT INTO delta.Scientific_Names (scientific_name)
                    VALUES (%s);
                """, (scientific_name))
                print(f"Inserted: {scientific_name}")
            else:
                print(f"Duplicate: {scientific_name}")


def insert_location(conn: 'connection', plant_df: pd.DataFrame) -> None:
    """Inserts the location of a plant into the database
        Ignores duplicate entries"""
    with conn.cursor() as cur:
        for index, row in plant_df.iterrows():
            latitude = row["Latitude"]
            longitude = row["Longitude"]
            town = row["Town"]
            country_code = row["Country_Code"]
            continent = row["Continent"]
            city = row["City"]

            cur.execute("""SELECT continent_id
                        FROM delta.Continents
                        WHERE continent_name = %s;
                        """, (continent))
            continent_id = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM delta.Locations
                WHERE latitude = %s AND longitude = %s AND town = %s
                        AND country_code = %s AND continent_id = %s AND city = %s;
            """, (latitude, longitude, town, country_code, continent_id, city))

            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                    INSERT INTO delta.Locations (latitude, longitude, town, country_code, continent_id, city)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (latitude, longitude, town, country_code, continent_id, city))
                print(
                    f"Inserted: ({latitude}, {longitude}, {town}, {country_code}, {continent_id}, {city})")
            else:
                print(f"""Duplicate: {latitude}, {longitude}, {
                      town}, {country_code}, {continent_id}, {city}""")


def insert_plants(conn: 'connection', plant_df: pd.DataFrame) -> None:
    """Inserts plant information into the database
        Ignores duplicate entries"""
    with conn.cursor() as cur:
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
                cur.execute("""SELECT continent_id
                            FROM delta.Continents
                            WHERE continent_name = %s;
                            """, (continent))
                continent_id = cur.fetchone()[0]
            except:
                continue
            if pd.isna(scientific_name):
                scientific_name = "None"
            scientific_name_id = find_scientific_name_id(
                conn, scientific_name)

            if pd.isna(image_url):
                image_url = "None"

            location_id = find_location_id(
                conn, latitude, longitude, town, country_code, continent_id, city)

            cur.execute("""
                SELECT COUNT(*)
                FROM delta.Plants
                WHERE plant_id = %s AND plant_name = %s AND scientific_id = %s AND location_id = %s
                        AND image_url = %s;
            """, (plant_id, plant_name, scientific_name_id, location_id, image_url))

            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                    INSERT INTO delta.Plants (plant_id, plant_name, scientific_id, location_id, image_url)
                    VALUES (%s, %s, %s, %s, %s);
                """, (plant_id, plant_name, scientific_name_id, location_id, image_url))
                print(
                    f"Inserted: ({plant_id}, {plant_name}, {scientific_name_id}, {location_id}, {image_url})")
            else:
                print(f"""Duplicate: ({plant_id}, {plant_name}, {
                    scientific_name_id}, {location_id}, {image_url})""")


def insert_recording(conn: 'connection', plant_df: pd.DataFrame) -> None:
    with conn.cursor() as cur:
        for index, row in plant_df.iterrows():
            plant_id = row["plant_id"]
            last_watered = row["last_watered"]
            soil_moisture = row["soil_moisture"]
            temperature = row["temperature"]
            reading_taken = row["recording_taken"]

            watered_datetime = datetime.strptime(
                last_watered, "%a, %d %b %Y %H:%M:%S %Z")

            cur.execute("""
                SELECT COUNT(*)
                FROM delta.Recordings
                WHERE plant_id = %s AND last_watered = %s AND soil_moisture = %s AND temperature = %s
                        AND reading_taken = %s;
            """, (plant_id, watered_datetime, soil_moisture, temperature, reading_taken))

            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                            INSERT INTO delta.Recordings (plant_id, last_watered, soil_moisture, temperature, reading_taken)
                            VALUES (%s, %s, %s, %s, %s);""", (plant_id, watered_datetime, soil_moisture, temperature, reading_taken))
                print(
                    f"Inserted: ({plant_id}, {watered_datetime}, {soil_moisture}, {temperature}, {reading_taken})")
            else:
                print(f"""Duplicate: ({plant_id}, {watered_datetime}, {
                      soil_moisture}, {temperature}, {reading_taken})""")


def insert_assignments(conn: 'connection', plant_df: pd.DataFrame) -> None:
    """Inserts the Botanist/Plant Assignment into the database."""
    with conn.cursor() as cur:
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
                conn, first_name, last_name, email, phone)
            if pd.isna(scientific_name):
                scientific_name = "None"
            scientific_name_id = find_scientific_name_id(
                conn, scientific_name)

            if pd.isna(image_url):
                image_url = "None"
            try:
                cur.execute("""SELECT continent_id
                                FROM delta.Continents
                                WHERE continent_name = %s;
                                """, (continent))
                continent_id = cur.fetchone()[0]
            except:
                continue
            location_id = find_location_id(
                conn, latitude, longitude, town, country_code, continent_id, city)
            plant_id = find_plant_id(
                conn, plant_name, scientific_name_id, location_id, image_url)

            cur.execute("""
                        SELECT COUNT(*)
                        FROM delta.Assignments
                        WHERE botanist_id = %s AND plant_id = %s;
                        """, (botanist_id, plant_id))
            count = cur.fetchone()[0]

            if count == 0:
                cur.execute("""
                            INSERT INTO delta.Assignments (botanist_id, plant_id)
                            VALUES (%s, %s);""", (botanist_id, plant_id))
                print(f"""Registered assignment of {first_name} {
                      last_name} {botanist_id} to {plant_name} {plant_id}""")
            else:
                print(f"""Duplicate assignment of {first_name} {
                      last_name} {botanist_id} to {plant_name} {plant_id} """)


if __name__ == "__main__":
    connection = get_connection()
    plant_data = load_csv()
    insert_botanists(connection, plant_data)
    insert_scientific_name(connection, plant_data)
    insert_location(connection, plant_data)
    insert_plants(connection, plant_data)
    insert_recording(connection, plant_data)
    insert_assignments(connection, plant_data)
