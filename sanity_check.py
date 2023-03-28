#%%
import pandas as pd 
import numpy as np

weather_df = pd.read_csv("data/SingaporeWeather.csv")
#%%
weather_df.dtypes
# %%
weather_df["Timestamp"] = pd.to_datetime(weather_df["Timestamp"])
# %%
weather_df["Temperature"].replace("M", np.NaN, inplace=True)
weather_df["Humidity"].replace("M", np.NaN, inplace=True)
# %%
weather_df["Temperature"] = weather_df["Temperature"].astype(float)
weather_df["Humidity"] = weather_df["Humidity"].astype(float)
# %%
weather_df.index = weather_df["Timestamp"]
# %%
weather_df.dtypes
#%%
weather_df.head()
# %%
weather_df[(weather_df["Station"] == "Paya Lebar") & (weather_df.index.year == 2020)]["Temperature"].groupby(by=pd.Grouper(freq='M')).max()
# %%
