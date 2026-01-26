import pandas as pd

# file_path = '100daysofcodePython/d16-d30/d25/weather_data.csv'

# data = []
# with open(file=file_path, mode='r') as f:
#     data = f.readlines()
# print(data)
# output -> ['day,temp,condition\n', 'Monday,12,Sunny\n', 'Tuesday,14,Rain\n', 'Wednesday,15,Rain\n', 'Thursday,14,Cloudy\n', 'Friday,21,Sunny\n', 'Saturday,22,Sunny\n', 'Sunday,24,Sunny']

# import csv

# TEMPERATURE_COLUMN_NUMBER = 1

# with open(file=file_path, mode='r') as f:
#     data = list(csv.reader(f))
#     temperatures = []
#     for row in data[1:]:
#         temperatures.append(int(row[TEMPERATURE_COLUMN_NUMBER]))
#     print(temperatures)
# output -> 12, 14, 15, 14, 21, 22, 24]


# data = pd.read_csv(filepath_or_buffer=file_path)

# print(data)

# data_dict = data.to_dict()
# print(data_dict)
# output -> {'day': {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}, 'temp': {0: 12, 1: 14, 2: 15, 3: 14, 4: 21, 5: 22, 6: 24}, 'condition': {0: 'Sunny', 1: 'Rain', 2: 'Rain', 3: 'Cloudy', 4: 'Sunny', 5: 'Sunny', 6: 'Sunny'}}

# data_list = data['temp'].to_list()
# print(data_list)
# ouput -> [12, 14, 15, 14, 21, 22, 24]

# avg of temperature
# averge = sum(data_list)/len(data_list)
# print(round(averge,2))

# print(data['temp'].mean())
# print(data['temp'].max())

# get rows data
# print(data[data['day'] == 'Monday'])

# which day had maximum temp
# print(data[data['temp'] == data['temp'].max()])

# create df from scratch
# data_dict = {
#     'students': ['A', 'B', 'C'],
#     'scores': [34, 22, 23]
# }
# file_path = '100daysofcodePython/d16-d30/d25/sample_df.csv'

# data_df = pd.DataFrame(data_dict)
# data_df.to_csv(path_or_buf=file_path)


file_path = '100daysofcodePython/d16-d30/d25/2018_Central_Park_Squirrel_Census_-_Squirrel_Data.csv'

data = pd.read_csv(filepath_or_buffer=file_path)

fur_colors = ['Cinnamon', 'Gray', 'Black']
counts = []

cinnamon_count = data[data['Primary Fur Color'] == 'Cinnamon']['Primary Fur Color'].count()
counts.append(cinnamon_count)
gray_count = len(data['Primary Fur Color'] == 'Gray')
counts.append(gray_count)
black_count = data[data['Primary Fur Color'] == 'Black']['Primary Fur Color'].count()
counts.append(black_count)

print(cinnamon_count)
print(gray_count)
print(black_count)

dict = {
    'Fur Color': fur_colors,
    'Count': counts
}

df = pd.DataFrame(dict)

output_path = '100daysofcodePython/d16-d30/d25/squirrel_count.csv'
df.to_csv(output_path)