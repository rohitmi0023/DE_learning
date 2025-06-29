# Article: https://quickstarts.snowflake.com/guide/getting_started_with_python/index.html#0

# pip install snowflake-connector-python

# Connection
import snowflake.connector

ctx = snowflake.connector.connect(
    user='',
    password='',
    account=''
)
cs = ctx.cursor()
try:
    cs.execute("select current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

# password through environment variables
import snowflake.connector

password = os.getenv('SNOWSQL_PWD')
conn = snowflake.connector.connect(
    user='',
    password=password,
    account=''
)

# Session parameters

conn = snowflake.connector.connect(
    user='',
    password='',
    account='',
    session_parameters={
        'QUERY_TAG': 'EndOfMonthFinancials',
    }
)
# alterntive way after connection setup
conn.cursor().execute("ALTER SESSION SET QUERY_TAG='EndOfMonthFinancials'")

# Create a warehouse
conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")

# create a database
conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb")

# create a schema
conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema")

# create a table
conn.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "test_table(col1 integer, col2 string)")

# insert data
conn.cursor().execute(
    "INSERT INTO test_table(col1, col2) "
    "VALUES(123, 'test string1'),(456, 'test string2')")
# for file type data
conn.cursor().execute("PUT file:///tmp/data/file* @%test_table")
conn.cursor().execute("COPY INTO test_table")

# Query data
col1, col2 = conn.cursor().execute('SELECT c1, c2 from table').fetchone()
print(f"{col1}, {col2}")
conn.close()