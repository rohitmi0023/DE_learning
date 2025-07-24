#%%

dic = {'name': 'rohit'}
def unpack(dic):
    print(dic)
    unpacked = set(dic)
    print(unpacked)
    print('bye')

unpack(dic)


# %%
def greet(name):
    print(f"Hello {name}")

greet(**dic)

# %%
import pandas as pd
# Creating Series
s = pd.Series([1, 2, 3, 4])
s_with_index = pd.Series([1, 2, 3], index=['a', 'b', 'c'])
s_with_dict = pd.Series({'a': 1, 'b': 2, 'c': 3})


# Basic Operations
print(s.values) # get values as numpy_array
print(s.index) # gets index
print(s.dtype) # data type

# %%
# DataFrame (2-dimensional labeled data structure)

# Creating Dataframe
data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000, 60000, 70000]
}

df = pd.DataFrame(data)

# from list of dictionaries
records = [
    {'name': 'Alice', 'age': 25, 'salary': 50000},
    {'name': 'Bob', 'age': 30, 'salary': 60000}
]
df = pd.DataFrame(records)

df = pd.read_csv('data.csv')

# explain loc and iloc
# .loc is label-based, meaning that you have to specify rows and columns based on their labels.
# .iloc is position-based, meaning that you have to specify rows and columns based on their integer positions.
# example:
df.loc[0:5, ['trip_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']]  # using loc to get rows 0 to 5 and specific columns
df.iloc[0:5, [0, 1, 2]]  # using iloc to get rows 0 to 5 and specific columns by index

# %%
# boolean indexing
df[df['age'] > 30]  # returns rows where age is greater than 30
df[df['name'].str.contains('A')] # names contining 'A'

# conditional selection with loc
df.loc[df['age'] > 30, ['name', 'salary']]

# %%
# data cleaning and transformation
df.isnull() # returns boolean dataframe indicating if values are null
df.issnull().sum() # count nulls per column
df.notnull() # returns boolean dataframe indicating if values are not null

# dropping missing data
df.dropna() # drop rows with any null values
df.dropna(subset=['age'])
df.dropna(axis=1) # drop columns with any null values

# Filling misssing data
df.fillna(0) 
df.fillna({'age': 0, 'salary': df['salary'].mean()})

# %%
# string operations
df['name'].str.lower()
# .contains('A')
# .startsWith('A')
# .len()

# %%
# merging and joining
df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
df2 = pd.DataFrame({'key': ['A', 'B', 'D'], 'value2': [4, 5, 6]})
merged_df = pd.merge(df1, df2, on='key', how='inner')

joined_df = df1.join(df2.set_index('key'), on='key')
