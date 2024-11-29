import pandas as pd

csv1 = pd.read_csv('./2024-11-27_plant_monitor_data.csv')
csv2 = pd.read_csv('./2024-11-28_plant_monitor_data.csv')

existing_df = pd.concat(
    [csv1, csv2], ignore_index=True)

existing_df['recording_taken'] = pd.to_datetime(
    existing_df['recording_taken'])

sorted_updated_df = existing_df.sort_values(
    by='recording_taken', ascending=False)

sorted_updated_df = sorted_updated_df.drop(columns=['plant_name', 'country_name', 'botanist_first_name', 'botanist_last_name',
                                                    'botanist_email', 'botanist_phone_number'])

print(sorted_updated_df)

sorted_updated_df.to_csv('./updated_recordings_data.csv', index=False)
