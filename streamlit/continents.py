# pylint: disable=no-member
"""Script for creating and returning
continents line chart to be used on streamlit dash."""

from os import environ
import altair as alt
import pandas as pd
import pymssql
from base_script import return_merged_df


def continents() -> alt.Chart:
    """Creates and returns the continents chart
    to be used on the streamlit dashboard."""
    alt.data_transformers.disable_max_rows()

    return_merged_df()

    def get_connection() -> pymssql.Connection:
        conn = pymssql.connect(
            server=environ["DB_HOST"],
            port=environ["DB_PORT"],
            user=environ["DB_USER"],
            password=environ["DB_PASSWORD"],
            database=environ["DB_NAME"],
            as_dict=True
        )

        return conn

    connection = get_connection()
    cursor = connection.cursor()
    print("connected")
    merged_df = return_merged_df()

    def get_continent(cursor):
        """Returns the continent of a given plant ID"""
        cursor.execute("""
            SELECT plants.plant_id, cont.continent_name 
            FROM delta.Continents AS cont
            JOIN delta.Locations AS loc
            ON cont.continent_id = loc.continent_id
            JOIN delta.Plants AS plants
            ON plants.location_id = loc.location_id
            ORDER BY plants.plant_id
        """)

        results = cursor.fetchall()
        if results:
            return results
        else:
            print("No results")
            return None

    continent_data = get_continent(cursor)

    continent_df = pd.DataFrame(continent_data)

    all_data = pd.merge(merged_df, continent_df, how="left", on=["plant_id"])

    all_data["recording_taken"] = pd.to_datetime(all_data["recording_taken"])

    all_data["recording_taken"] = all_data["recording_taken"].dt.floor("H")

    grouped_df = all_data.groupby(["recording_taken", "continent_name"]).agg(
        avg_soil_moisture=("soil_moisture", "mean")
    ).reset_index()

    print("Making Graph")

    chart = alt.Chart(grouped_df).mark_line(point=True).encode(
        x=alt.X("recording_taken:T", title="Time (Hour)"),
        y=alt.Y("avg_soil_moisture:Q", title="Average Soil Moisture"),
        color="continent_name:N",
        tooltip=["reading_taken:T", "continent_name:N", "avg_soil_moisture:Q"]
    ).properties(
        title="Average Soil Moisture Per Continent Over Time",
        width=800,
        height=400
    )

    return chart
