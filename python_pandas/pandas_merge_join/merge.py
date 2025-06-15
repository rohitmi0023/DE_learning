#%%
import pandas as pd
climate_temp = pd.read_csv("./climate_temp.csv")
climate_precip = pd.read_csv('./climate_precip.csv')
print('Dimensions in climate_temp:', climate_temp.shape)
print('Dimensions in climate_precip:', climate_precip.shape)
precip_one_station = climate_precip.query("STATION == 'GHCND:USC00045721'")
inner_merged = pd.merge(climate_temp, precip_one_station)
print('Dimensions in inner_merged:', inner_merged.shape)
inner_merged_total = pd.merge(climate_precip,climate_temp,how='inner',on=["STATION", "DATE"])
print('Dimensions in inner_merged_total:', inner_merged_total.shape)
outer_merged = pd.merge(climate_temp, precip_one_station, how='outer', on=["STATION", "DATE"])
print('Dimensions in outer_merged:', outer_merged.shape)

left_merged = pd.merge(climate_temp, precip_one_station, how='left', on=["STATION", "DATE"])
print('Dimensions in left_merged:', left_merged.shape)
left_merged_reversed = pd.merge(precip_one_station, climate_temp, how='left', on=["STATION", "DATE"])
print('Dimensions in left_merged_reversed:', left_merged_reversed.shape)
joined = precip_one_station.join(climate_temp, lsuffix="_left", rsuffix="_right")
print('Dimensions in joined:', joined.shape)
inner_joined_total = climate_temp.join(
    climate_precip.set_index(["STATION", "DATE"]),
    on=["STATION", "DATE"],
    how='inner',
    lsuffix="_left",
    rsuffix="_right",
)
print('Dimensions in inner_joined_total:', inner_joined_total.shape)
double_precip = pd.concat([precip_one_station, precip_one_station])
print('Dimensions in double_precip:', double_precip.shape)
reindexd = pd.concat([precip_one_station, precip_one_station], ignore_index=True)
print('Dimensions in reindexd:', reindexd.shape)
outer_joined = pd.concat([climate_precip,climate_temp])
print('Dimensions in outer_joined:', outer_joined.shape)
inner_joined = pd.concat([climate_precip, climate_temp], join='inner')
print('Dimensions in inner_joined:', inner_joined.shape)
inner_joined_cols = pd.concat([climate_precip, climate_temp], join='inner', axis=1)
print('Dimensions in inner_joined_cols:', inner_joined_cols.shape)
hierarchical_indexed = pd.concat([climate_precip, climate_temp], keys=['precip', 'temp'])
print('Indexes in hierarchical_indexed:', hierarchical_indexed.index)
# %%
