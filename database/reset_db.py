from os import environ
import pymssql
from dotenv import load_dotenv


def reset_db(conn):

    with conn.cursor() as cur:
        cur.execute("""

IF OBJECT_ID('delta.Recordings', 'U') IS NOT NULL
    DROP TABLE delta.Recordings;

IF OBJECT_ID('delta.Assignments', 'U') IS NOT NULL
    DROP TABLE delta.Assignments;

IF OBJECT_ID('delta.Plants', 'U') IS NOT NULL
    DROP TABLE delta.Plants;

IF OBJECT_ID('delta.Locations', 'U') IS NOT NULL
    DROP TABLE delta.Locations;

IF OBJECT_ID('delta.Continents', 'U') IS NOT NULL
    DROP TABLE delta.Continents;

IF OBJECT_ID('delta.Botanists', 'U') IS NOT NULL
    DROP TABLE delta.Botanists;

IF OBJECT_ID('delta.Scientific_Names', 'U') IS NOT NULL
    DROP TABLE delta.Scientific_Names;


CREATE TABLE delta.Continents (
    continent_id INT NOT NULL UNIQUE,
    continent_name VARCHAR(30) NOT NULL UNIQUE,
    primary key (continent_id)
);

INSERT INTO delta.Continents (continent_id, continent_name) VALUES
(1, 'America'),
(2, 'Asia'),
(3, 'Antarctica'),
(4, 'Europe'),
(5, 'Africa'),
(6, 'Pacific')
;

CREATE TABLE delta.Locations (
    location_id INT IDENTITY (1, 1) PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    continent_id INT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    city VARCHAR(25),
    town VARCHAR(25),
    FOREIGN KEY (continent_id) REFERENCES delta.Continents (continent_id)
);

CREATE TABLE delta.Scientific_Names (
    scientific_id INT IDENTITY (0, 1) PRIMARY KEY,
    scientific_name VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO delta.Scientific_Names (scientific_name) VALUES ('None');

CREATE TABLE delta.Botanists (
    botanist_id INT IDENTITY (1, 1) PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    email VARCHAR(50) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE delta.Plants (
    plant_id INT UNIQUE NOT NULL,
    plant_name VARCHAR(30),
    scientific_id INT,
    location_id INT NOT NULL,
    image_url VARCHAR(300),
    primary key (plant_id),
    FOREIGN KEY (scientific_id) REFERENCES delta.Scientific_Names (scientific_id),
    FOREIGN KEY (location_id) REFERENCES delta.Locations (location_id)
);

CREATE TABLE delta.Recordings (
    recording_id INT IDENTITY (1, 1) PRIMARY KEY,
    plant_id INT NOT NULL,
    last_watered datetime2,
    soil_moisture FLOAT NOT NULL,
    temperature FLOAT NOT NULL,
    reading_taken datetime2 NOT NULL,
    FOREIGN KEY (plant_id) REFERENCES delta.Plants (plant_id)
);

CREATE TABLE delta.Assignments (
    assignment_id INT IDENTITY (1, 1) PRIMARY KEY,
    botanist_id INT NOT NULL,
    plant_id INT NOT NULL,
    FOREIGN KEY (botanist_id) REFERENCES delta.Botanists (botanist_id),
    FOREIGN KEY (plant_id) REFERENCES delta.Plants (plant_id)
);
""")

    conn.commit()


if __name__ == "__main__":
    load_dotenv()

    conn = pymssql.connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"],
        as_dict=True
    )

    reset_db(conn)
