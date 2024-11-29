import streamlit as st
import altair as alt
import pandas as pd
from base_script import return_merged_df


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
    ].copy()  # Explicitly create a copy to avoid the warning
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


if __name__ == "__main__":
    main()
