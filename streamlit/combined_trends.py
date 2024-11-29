"""Creates combined trends (soil moisture, temperature)
line chart over time."""
import altair as alt
import pandas as pd
from base_script import return_merged_df


def combined_trends_graph() -> alt.Chart:
    """Makes the line chart and returns it to dashboard
    script."""
    merged_df = return_merged_df()

    alt.data_transformers.disable_max_rows()
    merged_df = merged_df.drop(
        columns=['recording_id', 'plant_id', 'last_watered'])

    merged_df.set_index('recording_taken', inplace=True)
    grouped_df = merged_df.resample('30min').mean()
    grouped_df = grouped_df.interpolate(method='linear').reset_index()

    long_df = grouped_df.melt(
        id_vars='recording_taken',
        value_vars=['soil_moisture', 'temperature'],
        var_name='Measurement',
        value_name='Value'
    )

    # Create the Altair line chart
    combined_trends_chart = alt.Chart(long_df).mark_line().encode(
        x=alt.X('recording_taken:T', title='Time'),
        y=alt.Y('Value:Q', title='Value'),
        color='Measurement:N',
        tooltip=['recording_taken:T', 'Measurement:N', 'Value:Q']
    ).properties(
        title='30-Minute Average Soil Moisture and Temperature over Time',
        width=800,
        height=400
    )

    return combined_trends_chart
