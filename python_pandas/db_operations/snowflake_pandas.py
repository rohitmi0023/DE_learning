# Article: https://docs.snowflake.com/developer-guide/python-connector/python-connector-pandas

# pip install "snowflake-connector-python[pandas]"

# reading from snowflake db to pandas df
import snowflake_python_connector

# conn = snowflake_python_connector.conn
cur = conn.cursor()
sql = 'select * from table'
cur.execute(sql)
df = cur.fetch_pandas_all()

# writing data from pandas df to snowflake db