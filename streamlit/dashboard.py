from os import environ
import streamlit as st
import altair as alt
import pandas as pd
import pymssql
from base_script import return_merged_df
from combined_trends import combined_trends_graph


def load_data():
    """Load the dataset from the base script."""
    return return_merged_df()


def filter_data(df, plant_id, start_time, end_time):
    """
    Filter the dataframe for the selected plant and time range, grouping by minute.
    """
    df["recording_taken"] = pd.to_datetime(
        df["recording_taken"], errors="coerce")
    filtered_df = df.loc[
        (df["plant_id"] == plant_id)
        & (df["recording_taken"] >= start_time)
        & (df["recording_taken"] <= end_time)
    ].copy()
    filtered_df["minute"] = filtered_df["recording_taken"].dt.strftime(
        "%Y-%m-%d %H:%M")
    average_readings = (
        filtered_df.groupby("minute")
        .agg({"soil_moisture": "mean", "temperature": "mean"})
        .reset_index()
    )
    return average_readings


def get_last_watered(df):
    """
    Get the last watered timestamp for each plant.
    """
    df["recording_taken"] = pd.to_datetime(
        df["recording_taken"], errors="coerce")
    last_watered = (
        df.groupby("plant_id", as_index=False)
        .agg({"recording_taken": "max"})
        .rename(columns={"recording_taken": "last_watered"})
    )
    return last_watered


def prepare_data_for_chart(df):
    """
    Melt the dataframe for chart compatibility.
    """
    return df.melt(id_vars="minute", var_name="Measurement", value_name="Value")


def create_chart(data):
    """
    Create a stacked bar chart for soil moisture and temperature.
    """
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("minute:T", title="Time (by Minute)",
                    axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("Value:Q", stack="zero", title="Readings"),
            color=alt.Color("Measurement:N",
                            legend=alt.Legend(title="Measurement")),
            tooltip=["minute:T", "Measurement:N", "Value:Q"],
        )
        .properties(
            title="Average Soil Moisture and Temperature (Stacked Bars by Minute)",
            width=800,
            height=400,
        )
    )
    return chart


def create_last_watered_chart(last_watered_data):
    """
    Create a bar chart for the last watered times for all plants.
    """
    chart = (
        alt.Chart(last_watered_data)
        .mark_bar()
        .encode(
            x=alt.X("plant_id:O", title="Plant ID"),
            y=alt.Y("last_watered:T", title="Last Watered Time"),
            tooltip=["plant_id:O", "last_watered:T"],
        )
        .properties(
            title="Last Watered Time for All Plants",
            width=800,
            height=400,
        )
    )
    return chart


def continents():
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


def main():
    """Main function to run the Streamlit app."""
    st.title("Plant Data Dashboard")

    df = load_data()

    plant_id = st.selectbox("Select Plant ID:", df["plant_id"].unique())

    df["recording_taken"] = pd.to_datetime(df["recording_taken"])

    min_time = df["recording_taken"].min(
    ).to_pydatetime()
    max_time = df["recording_taken"].max().to_pydatetime()
    start_time, end_time = st.slider(
        "Select Time Range:",
        min_value=min_time,
        max_value=max_time,
        value=(min_time, max_time),
        format="YYYY-MM-DD HH:mm",
        step=pd.Timedelta(minutes=1).to_pytimedelta(),
    )

    average_readings = filter_data(df, plant_id, start_time, end_time)
    if average_readings.empty:
        st.warning("No data available for the selected time range.")
        return
    chart_data = prepare_data_for_chart(average_readings)

    chart = create_chart(chart_data)
    st.altair_chart(chart, use_container_width=True)

    last_watered_data = get_last_watered(df)
    last_watered_chart = create_last_watered_chart(last_watered_data)
    st.altair_chart(last_watered_chart, use_container_width=True)

    continents_chart = continents()
    st.altair_chart(continents_chart, use_container_width=True)

    combined_trends_chart = combined_trends_graph()
    st.altair_chart(combined_trends_chart, use_container_width=True)


if __name__ == "__main__":
    main()
