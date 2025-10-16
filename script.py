import requests
import openai
import os
import time
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


# Loads the environment variables from the .env file
load_dotenv()

# Gets the POLYGON_API KEY from the .env file
API_KEY = os.getenv("POLYGON_API_KEY")



# Limit for each request page
LIMIT = 1000

# Create engine to communicate with snowflake

def run_stock_job():
    # URL to get the ticker from the Polygon API
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}"

    # List to store the tickers
    tickers_rows = []

    # HTTP GET request to the POLYGON API
    response = requests.get(url)

    # Converts the response to a JSON Object
    data = response.json()

    # Extract the data keys that are going to be our Snowflake table columns
    headers = list(data['results'][0].keys())

    # Loop to add each ticker from data to the ticker list
    for ticker in data['results']:
        tickers_rows.append(ticker)


    while 'next_url' in data:

        print(f"Requesting next page: {data['next_url']}")

        # Loop to add each ticker from data to the ticker list
        for ticker in data['results']:
            tickers_rows.append(ticker)

        # HTTP GET request to the Polygon API
        response = requests.get(f"{data['next_url']}&apiKey={API_KEY}")

        # Converts the response to a JSON Object
        data = response.json()

        # time to be idle in order to be able to not exceed requests per minute
        time.sleep(10)

    load_table_snowflake(tickers_rows, headers)
    

def load_table_snowflake(rows, headers):

    # Read Snowflake credentials from .env
    USER = os.getenv("SNOWFLAKE_USERNAME")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

    con = None
    cur = None
    table_name = 'STOCK_TICKERS'
    try:
        # Connect using Snowflake connector
        con = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )

        # Cursor to interact with Snowflake
        cur = con.cursor()

        # Ensure we are in the intended context
        if WAREHOUSE:
            cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
        if DATABASE:
            cur.execute(f"USE DATABASE {DATABASE}")
        if SCHEMA:
            cur.execute(f"USE SCHEMA {SCHEMA}")

        # Create DataFrame and sanitize column names
        df_tickers = pd.DataFrame(rows, columns=headers)
        
        # Uppercase columns names
        columns = []
        for col in headers:
            columns.append(col.upper())

        # Assign columns names to the dataframe
        df_tickers.columns = columns

        # Load dataframe into Snowflake using write_pandas
        result = write_pandas(
            conn=con,
            df= df_tickers,
            table_name=table_name
        )

        if result[0]:
            print("SUCCEEDED: Dataframe loaded into Snowflake table 'STOCK_TICKERS'")
        else:
            print("FAILED: Dataframe wasn't loaded into Snowflake table 'STOCK_TICKERS'")

    except Exception as e:
        print("Snowflake load failed:", e)
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

    

# This makes functions not run when imported
if __name__ == '__main__':
    run_stock_job()