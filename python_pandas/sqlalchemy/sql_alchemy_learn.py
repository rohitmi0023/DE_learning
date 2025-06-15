#%%
# SAVING THE PANDAS DF(FROM CSV) INTO A SQL TABLE
import pandas as pd
print('pd version: ', pd.__version__)
import sqlite3
print('sqlite3 version: ', sqlite3.sqlite_version)
# import psycopg2 
import sqlalchemy as sa
print('sqlalchemy version: ', sa.__version__)


# here we create a sqlite db using sqlalchemy as didn't exist already
sqlite_engine = sa.create_engine("sqlite:///rohit_sqlite_db.db")

# here we load csv file data into a df
df_air_pollution = pd.read_csv("air_pollution new.csv")

# next 3 lines covert year columns values from string to numbers and drop records where any of the column in null 
columns = ['2017', '2018', '2019', '2020', '2021', '2022', '2023']
for col in columns:
    df_air_pollution[col] = pd.to_numeric(df_air_pollution[col], errors = 'coerce')
    df_air_pollution.dropna(subset=[col], how='any', inplace=True)

# here we convert our df data into a table in SQL DB
df_air_pollution.to_sql("pollution_table", sqlite_engine, if_exists='replace')

# here we check what is there in the db file
sqlite_conn = sqlite3.connect("rohit_sqlite_db.db")
cursor = sqlite_conn.cursor()
cursor.execute("SELECT name from sqlite_master WHERE type='table';")
print(cursor.fetchall())
cursor.execute("SELECT * FROM pollution_table where country='India' and city like '%Mumbai%' limit 10;")
print(cursor.fetchall())


# %%
# LOADING THE SQL TABLE USING PANDAS
import pandas as pd
# import psycopg2 as pg2
# combination of "psyco"(from python just-in-time compiler "Psyco") and postgreSQL/pg, with "2" representing version 2 of the python db API.
import sqlalchemy as sa


# connecting to sqlite db file
sqlite_engine = sa.create_engine("sqlite:///rohit_sqlite_db.db")

# reading table structure
table_structure = pd.read_sql("PRAGMA table_info(pollution_table)", con=sqlite_engine)
# print(table_structure)

# reading the table from db and loading it into df
columns = ['city','country','2017','2018','2019','2020','2021','2022','2023']
df_table = pd.read_sql_table('pollution_table', con=sqlite_engine,columns=columns)
# alternative to above
df_db = pd.read_sql("SELECT * from pollution_table", con=sqlite_engine)

# operations on df
# 1. Average of top 5 cities
avg_air = df_table['2023'].mean(axis=0).round(2)
print(f'Average AQI of cities in 2023 year: {avg_air}')
# 2. Visualizing
# worst 5 cities, using double qoutes for numeric column identifiers
df_worst_5 = pd.read_sql('select city, "2023" from pollution_table order by "2023" desc limit 5', con=sqlite_engine)
# df_worst_5.plot(x="city", y="2023", kind="bar")
# worst 3 countries
df_worst_3_countries = pd.read_sql('select country, avg("2023") as AQI from pollution_table group by country order by 2 desc limit 3', con=sqlite_engine)
df_worst_3_countries.plot(x="country", y="AQI", kind="bar")


# %%
# DATACAMP ARTICLE
import sqlalchemy as sa
print('sqlalchemy version: ', sa.__version__)
import sqlite3 
print('sqlite versions: ', sqlite3.sqlite_version)
import pandas as pd

# # connecting to a sqlite db file
# sqlite_engine = sa.create_engine("sqlite:///european_database.sqlite")
# connection = sqlite_engine.connect()

# # reading db
# sqlite_connection = sqlite3.connect("european_database.sqlite")
# cursor = sqlite_connection.cursor()
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor.fetchall())

# sqlite_engine = sa.create_engine('sqlite:///rohit.sqlite')
# connection = sqlite_engine.connect()
# metadata = sa.MetaData()
# Student = sa.Table('Student',
#           metadata, 
#           sa.Column('Id', sa.Integer(), primary_key=True),
#           sa.Column('Name', sa.String(255), nullable=False),
#           sa.Column('Major', sa.String(255), default="Math"),
#           sa.Column('Pass', sa.Boolean(), default=True)
#         )
# metadata.create_all(sqlite_engine)

# query = sa.insert(Student).values(Id=1, Name='Matthew', Major="English", Pass=True)
# Result = connection.execute(query)
# output = connection.execute(Student.select()).fetchall()
# print(output)