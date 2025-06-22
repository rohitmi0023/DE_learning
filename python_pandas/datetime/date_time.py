# %%
import pandas as pd
import matplotlib.pyplot as plt

air_quality = pd.read_csv("air_quality_no2_long.csv")
dict = {"date.utc": "datetime"}
air_quality.rename(columns=dict, inplace=True)
air_quality['datetime'] = pd.to_datetime(air_quality["datetime"])

# 1. What is the start and end date of the time series data set we are working with?
air_quality['datetime'].min()
air_quality['datetime'].max()

# 2. I want to add a new column to the DataFrame containing only the month of the measurement
air_quality['month'] = air_quality['datetime'].dt.month

# 3. What is the average NO2  concentration for each day of the week for each of the measurement locations?
air_quality.groupby([ air_quality['datetime'].dt.weekday, 'location'])['value'].mean()

# 4. Plot the typical NO2 pattern during the day of our time series of all stations together. In other words, what is the average value for each hour of the day?
fig, axs = plt.subplots(figsize=(12,4))
air_quality.groupby(air_quality['datetime'].dt.hour)['value'].mean().plot(kind='bar', rot=0, ax=axs)
plt.xlabel("Hour of the day")
plt.ylabel("$NO2")

# %%
import pandas as pd
import matplotlib.pyplot as plt

air_quality = pd.read_csv("air_quality_no2_long.csv")
dict = {"date.utc": "datetime"}
air_quality.rename(columns=dict, inplace=True)
air_quality['datetime'] = pd.to_datetime(air_quality["datetime"])

# Datetime as index
no_2 = air_quality.pivot(index='datetime', columns='location', values='value')
# Create a plot of the NO2 values in the different stations from the 20th of May till the end of 21st of May
no_2['2019-05-20':'2019-05-21'].plot()



# Resample a time series to another frequency
# Aggregate the current hourly time series values to the monthly maximum value in each of the stations.
monthly_max = no_2.resample('ME').max()
monthly_max.index.freq
