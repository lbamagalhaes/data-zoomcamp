# ------------------------------------------------------------------------------------------

# QUESTION 1:

# docker --help build

# --iidfile string contains the following tag: "Write the image ID to the file"

# ------------------------------------------------------------------------------------------

# QUESTION 2:

# docker -run -it --entrypoint=bash python:3.9
# pip list

# ------------------------------------------------------------------------------------------

# QUESTION 3:

#import packages
import pandas as pd

#import data
df_green = pd.read_csv('green_tripdata_2019-01.csv')
df_zone = pd.read_csv('taxi+_zone_lookup.csv')

# filter lpep_pickup_datetime and lpep_dropoff_datetime = 2019-01-15
question_3 = df_green[(df_green['lpep_pickup_datetime'] >= '2019-01-15 00:00:00') \
                      & (df_green['lpep_dropoff_datetime'] <= '2019-01-15 23:59:59')].shape[0]

print(question_3)

# ------------------------------------------------------------------------------------------

# QUESTION 4:

question_4 = list(
  df_green.sort_values(
    by='trip_distance',ascending=False
  )['lpep_pickup_datetime']
)[0]

print(question_4)

# ------------------------------------------------------------------------------------------

# QUESTION 5:

passengers_2 = df_green[
  (df_green['lpep_pickup_datetime'] >= '2019-01-01 00:00:00') \
  & (df_green['lpep_pickup_datetime'] <= '2019-01-01 23:59:59') \
  & (df_green['passenger_count'] == 2)]

passengers_3 = df_green[
  (df_green['lpep_pickup_datetime'] >= '2019-01-01 00:00:00') \
  & (df_green['lpep_pickup_datetime'] <= '2019-01-01 23:59:59') \
  & (df_green['passenger_count'] == 3)]

print(f"2: {passengers_2}, 3: {passengers_3} ")

# ------------------------------------------------------------------------------------------

# QUESTION 6:

# get Astoria location_id:
astoria_location_id = list(df_zone[df_zone['Zone'] == 'Astoria']['LocationID'])[0]

#for PULLocationID = 7, get the drop off zone that had the largest tip
drop_off_zone_id = list(df_green[df_green['PULocationID'] == astoria_location_id]\
                        .sort_values(by='tip_amount',ascending=False)['DOLocationID'])[0]

# get location name for  DOLocationID = 146:
astoria_location_name

print()= list(df_zone[df_zone['LocationID'] == drop_off_zone_id]['Zone'])[0]

question_6 = astoria_location_name

print(question_6)



# ------------------------------------------------------------------------------------------