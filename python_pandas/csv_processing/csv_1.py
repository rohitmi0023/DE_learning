# %%
import csv

# how to read a csv file
csv_file_path = 'day.csv'
with open(csv_file_path) as my_csv:
    csv_reader = csv.reader(my_csv, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print('Headers: ', ', '.join(row))
            line_count += 1
        else:
            print('Day Number in numeric: ', row[0], ', Day Number in Numeric Suffix: ', row[2])
            line_count += 1
    print('Rows processed: ', line_count)
    
# %%
# Reading csv file in as a dictionary

with open(csv_file_path) as my_csv:
    csv_reader = csv.DictReader(my_csv)
    line_count = 0
    for row in csv_reader:
        print(row)
        if line_count == 0:
            print('Headers are: ', ', '.join(row))
            line_count += 1
        else:
            print('Day Number in numeric: ', row['Numeric'], ' Day number in suffix: ', row['Numeric-Suffix'])
            line_count += 1
    print('Processed lines: ', line_count)

# %%
# Writing to csv file

with open(csv_file_path,mode='a') as my_csv:
    csv.writer(my_csv, quotechar='"').writerow(['32','32','32nd'])

# %%
# DictWriter

with open(csv_file_path,mode='w') as csv_file:
    fieldnames = ['Numeric','Numeric-2','Numeric-Suffix']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    row1 = {'Numeric': '1','Numeric-2': '01','Numeric-Suffix': '1st'}
    writer.writerow(row1)

# %%
import pandas as pd

# If your CSV files doesn’t have column names in the first line, you can use the names optional parameter to provide a list of column names. 
# You can also use this if you want to override the column names provided in the first line. 
# In this case, you must also tell pandas.read_csv() to ignore existing column names using the header=0 optional parameter:
df = pd.read_csv(csv_file_path, index_col='Numeric', parse_dates=[''], header=0,names=[''])

df.to_csv('newer.csv')
